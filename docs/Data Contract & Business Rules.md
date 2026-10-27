# Data Contract & Business Rules: Cross-Border Payment Reconciliation

Dokumen ini adalah kontrak resmi (*Single Source of Truth*) yang mengatur skema data, aturan validasi, penanganan anomali teknis di hulu (PySpark), dan logika rekonsiliasi bisnis di hilir (dbt).

---

## 1. Skema Sumber Data (Data Contracts)

### A. Skema Jalur Cepat: Webhook JSON (`raw_webhooks`)
Log peristiwa *webhook* yang diterima secara *micro-batch* dari gateway pembayaran.

| Path JSON | Tipe Data | Wajib (Not Null) | Keterangan & Batasan |
| :--- | :--- | :---: | :--- |
| `event_id` | String | Ya | Kunci unik pengiriman log (`evt_<alphanumeric>`). |
| `timestamp` | Timestamp (ISO8601) | Ya | Waktu penerimaan log di sistem internal (`UTC`). |
| `log_level` | String | Ya | Nilai: `INFO`, `WARN`, `ERROR`. |
| `environment` | String | Ya | Nilai: `production`, `sandbox`. |
| `event.category` | String | Ya | Kategori peristiwa (misal: `payment`). |
| `event.action` | String | Ya | Aksi peristiwa (misal: `payment_status_updated`). |
| `event.status` | String | Ya | Status transaksi: `pending`, `success`, `failed`, `cancelled`. |
| `transaction.transaction_id` | String | Ya | Kunci unik bisnis transaksi (`tx_<id>`). Digunakan untuk deduplikasi state dan rekonsiliasi. |
| `transaction.reference_id` | String | Ya | Nomor referensi pesanan/invoice dari merchant. |
| `transaction.gateway_name` | String | Ya | Nama gateway penyedia layanan (misal: `midtrans`, `stripe`). |
| `transaction.payment_method` | String | Ya | Metode bayar: `credit_card`, `qris`, `bank_transfer`, `e_wallet`. |
| `transaction.amount` | Decimal(18, 2) | Ya | Nilai nominal transaksi asli (harus bernilai positif `> 0`). |
| `transaction.currency` | String (ISO 4217) | Ya | Kode mata uang asal: `USD`, `SGD`, `EUR`, `IDR`. |
| `transaction.attempts` | Array[Struct] | Tidak | Riwayat percobaan pembayaran sebelum mencapai status akhir. |
| `transaction.attempts[].attempt_no` | Integer | Ya | Urutan percobaan (1, 2, ...). |
| `transaction.attempts[].status` | String | Ya | Status percobaan: `failed`, `cancelled`, `success`. |
| `transaction.attempts[].attempt_time` | Timestamp (ISO8601) | Ya | Waktu percobaan dilakukan. |
| `transaction.attempts[].reason` | String | Tidak | Alasan kegagalan (misal: `user_cancelled_3ds`, `insufficient_funds`). |
| `payment_gateway_response.status_code` | String | Ya | Kode respon HTTP gateway (misal: `200`, `400`). |
| `payment_gateway_response.gateway_status` | String | Ya | Status internal gateway (misal: `settlement`, `deny`). |
| `payment_gateway_response.approval_code` | String | Tidak | Kode persetujuan bank acquirer jika menggunakan kartu kredit. |
| `actor.user_id` | String | Ya | ID unik pengguna/pelanggan. |
| `actor.ip_address` | String | Tidak | Alamat IP pengguna. |
| `trace.trace_id` | String | Ya | ID penelusuran terdistribusi untuk observabilitas. |
| `trace.service_name` | String | Ya | Layanan pengirim log (misal: `payment-service`). |

---

### B. Skema Jalur Lambat 1: Bank Settlement CSV (`raw_bank_settlements`)
Laporan mutasi pencairan dana berkala dari bank mitra/acquirer.

| Nama Kolom CSV | Tipe Data Target | Wajib (Not Null) | Keterangan & Batasan |
| :--- | :--- | :---: | :--- |
| `settlement_id` | String | Ya | Primary Key dari pihak bank penerbit mutasi. |
| `transaction_id` | String | Ya | Foreign Key penghubung ke `transaction.transaction_id` internal. |
| `transaction_date` | Date | Ya | Tanggal transaksi terjadi menurut catatan bank. |
| `settlement_date` | Date | Ya | Tanggal uang riil ditransfer/cair ke rekening perusahaan. |
| `gross_amount` | Decimal(18, 2) | Ya | Nilai kotor transaksi setelah dikonversi ke mata uang pencairan. |
| `fee_amount` | Decimal(18, 2) | Ya | Potongan biaya admin/MDR bank mitra (`gross_amount * fee_rate`). |
| `net_settled_amount` | Decimal(18, 2) | Ya | Nilai bersih riil yang masuk ke rekening (`gross_amount - fee_amount`). |
| `settlement_currency` | String (ISO 4217) | Ya | Mata uang pencairan di bank (standar: `IDR`). |
| `applied_fx_rate` | Decimal(12, 4) | Ya | Nilai tukar kurs yang diterapkan bank saat mencairkan dana. |
| `bank_name` | String | Ya | Entitas bank mitra (misal: `BCA`, `MANDIRI`, `CITI`). |

---

### C. Skema Jalur Lambat 2: Kurs Mata Uang Harian CSV (`raw_fx_rates`)
Tabel referensi nilai tukar harian resmi (misal: kurs tengah Bank Indonesia / ECB).

| Nama Kolom CSV | Tipe Data Target | Wajib (Not Null) | Keterangan & Batasan |
| :--- | :--- | :---: | :--- |
| `rate_date` | Date | Ya | Tanggal berlakunya nilai tukar. |
| `base_currency` | String (ISO 4217) | Ya | Mata uang asal (misal: `USD`, `SGD`, `EUR`). |
| `target_currency` | String (ISO 4217) | Ya | Mata uang tujuan konversi (standar: `IDR`). |
| `exchange_rate` | Decimal(12, 4) | Ya | Nilai kurs konversi (1 Unit Base = X Unit Target). |

---

## 2. Aturan Penanganan Anomali Hulu (PySpark / Lapisan Silver)

### A. Karantina Teknis (Dead-Letter Queue / DLQ)
Data yang mengalami kerusakan teknis sistematis **dilarang** masuk ke tabel Silver utama dan wajib dipisahkan ke tabel `silver.quarantine_webhooks` dengan mencatat `rejection_reason`:
1. **Invalid/Malformed Structure:** Baris yang gagal di-parse sebagai JSON valid.
2. **Missing Business Identity:** Baris dengan `transaction.transaction_id IS NULL` atau bernilai string kosong.
3. **Invalid Monetary Value:** Nilai `transaction.amount IS NULL` atau `transaction.amount <= 0`.
4. **Missing Timestamp:** Nilai `timestamp IS NULL`.

### B. Deduplikasi dan Idempotensi Data
1. **Exact Duplicate Delivery:** Jika terdapat lebih dari satu log dengan `event_id` yang identik, simpan hanya satu baris pertama (`dropDuplicates(['event_id'])`).
2. **State Evolution Resolution:** Jika satu `transaction_id` memiliki beberapa peristiwa pembaruan status (`pending` -> `success`), ambil baris dengan status terbaru berdasarkan:
   $$\text{ROW\_NUMBER() OVER (PARTITION BY transaction\_id ORDER BY timestamp DESC)}$$

---

## 3. Aturan Bisnis Rekonsiliasi Hilir (dbt / Lapisan Gold)

### A. Formula Rekonsiliasi & Ekspektasi Nilai Bersih
Untuk setiap transaksi berstatus `success`, sistem menghitung nilai bersih yang diharapkan (*Expected Net Amount*):

$$\text{Expected Gross IDR} = \text{amount (asli)} \times \text{exchange\_rate (pada tanggal transaksi)}$$

$$\text{Expected Net IDR} = \text{Expected Gross IDR} - \text{Contracted Fee IDR}$$

$$\text{Variance IDR} = |\text{Expected Net IDR} - \text{net\_settled\_amount (Bank)}|$$

$$\text{Variance Percentage} = \frac{\text{Variance IDR}}{\text{Expected Net IDR}} \times 100\%$$

### B. Kebijakan Threshold Rekonsiliasi (Dynamic Tolerance)
* **Transaksi Domestik (`IDR` ke `IDR`):**
  * Ambang batas toleransi pembulatan: **$\le 0.05\%$** atau selisih nominal $\le \text{Rp } 100$.
* **Transaksi Lintas Negara (*Cross-Border* - `USD`/`SGD`/`EUR` ke `IDR`):**
  * Ambang batas toleransi pergeseran kurs harian (*FX Slippage*): **$\le 1.0\%$**.

### C. Klasifikasi Status Rekonsiliasi (Output Klasifikasi dbt)

| Status Rekonsiliasi | Definisi Kondisi Bisnis | Tabel Target di Gold |
| :--- | :--- | :--- |
| **`EXACT_MATCH`** | Transaksi ada di internal & bank, nilai `Variance IDR == 0`. | `fct_reconciliation` |
| **`MATCHED_WITHIN_THRESHOLD`** | Transaksi ada di kedua sisi, `Variance Percentage <= 1.0%`. | `fct_reconciliation` |
| **`AMOUNT_MISMATCH`** | Transaksi ada di kedua sisi, tetapi selisih **melebihi threshold** ($> 1.0\%$). | `fct_discrepancy_audit` |
| **`UNSETTLED_INTERNAL`** | Transaksi internal sukses, belum tercatat di mutasi bank ($< 3$ hari = *Late Arriving*, $> 3$ hari = *Overdue*). | `fct_discrepancy_audit` |
| **`MISSING_IN_INTERNAL`** | Uang masuk di mutasi bank, tetapi `transaction_id` tidak ada di catatan sistem aplikasi (*Ghost Payment*). | `fct_discrepancy_audit` |
| **`STATUS_MISMATCH`** | Internal mencatat `failed`/`cancelled`, tetapi bank mencairkan dana (*Zombie Payment*). | `fct_discrepancy_audit` |
