# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Duy Trọng  **Mã SV:** 2A202601333 **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy của <code>make verify</code></summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 10.9s
  run 2/3 … 11.3s
  run 3/3 … 11.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt (100%)**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Phiếu #1041: Bảng `gold_training_set` tăng số lượng hàng sau mỗi lần bấm Clear Task trên Airflow hoặc mỗi lần chạy lại pipeline, dù dữ liệu nguồn không tăng thêm ticket. |
| **Nguyên nhân** | Model `gold_training_set.sql` được cấu hình `materialized = 'incremental'` nhưng không khai báo `unique_key`. Khi thiếu `unique_key`, dbt mặc định sinh ra câu lệnh `INSERT` thuần (`append` strategy). Khi một ticket có bản ghi cập nhật (`op = 'u'`) rơi vào partition ngày khác hoặc khi chạy lại cùng một partition ngày, dbt chèn thêm các hàng mới thay vì ghi đè. Ngoài ra, DAG Airflow đặt `catchup=True` khiến việc Clear Task kích hoạt chạy lại toàn bộ các partition lịch sử trong quá khứ mà không giới hạn concurrency (`max_active_runs=None`), dẫn đến việc nhân bản dữ liệu hàng loạt. |
| **Cách khắc phục** | - `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'` vào khối `config()`.<br>- `dags/ai_training_pipeline.py`: Cập nhật `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38.750 hàng (12.480 ticket bị lặp, checksum 3 lượt thay đổi: `7c461563f4`, `d11657ff21`, `2b76a4f850`) · sau: **12.480 hàng** (đúng 1 hàng / 1 ticket, không lặp) · checksum 3 lượt ổn định: `8dd7c98653` |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Phiếu #1043: `gold_feature_daily` bị thiếu khoảng 5% số hàng ở các ngày quá khứ so với đối chiếu thủ công, trong khi ngày mới nhất thì đủ. |
| **P99 độ trễ đo được** | **2.73 ngày** *(chính xác: 2.7258 ngày $\approx$ 65.4 giờ; P50 = 0.13 ngày, P95 = 1.81 ngày, Max = 2.94 ngày, tỷ lệ trễ > 1 ngày: 5.05%)* |
| **Lookback đã chọn** | **3 ngày** — vì phân bố độ trễ cho thấy $P_{99} \approx 2.73\text{ ngày}$ và độ trễ lớn nhất $\text{Max} \approx 2.94\text{ ngày} < 3\text{ ngày}$. Lookback window 3 ngày đảm bảo bắt trọn 100% các bản ghi về muộn trong thực tế mà vẫn tối ưu chi phí tính toán. |
| **Nguyên nhân** | Model `gold_feature_daily.sql` sử dụng điều kiện lọc incremental `where event_date > (select max(event_date) from {{ this }})`. Khi một sự kiện xảy ra ngày 08-12 nhưng bị trễ đường truyền và đến kho ngày 08-15, lúc này `max(event_date)` trong target đã là 08-14, khiến sự kiện ngày 08-12 bị điều kiện lọc bỏ qua hoàn toàn ở mọi lượt chạy sau. |
| **Cách khắc phục** | - `dbt/models/gold/gold_feature_daily.sql`: Đổi điều kiện lọc incremental thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`.<br>- Đồng thời bổ sung `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` để các partition ngày được tính lại sẽ ghi đè chính xác kết quả thay vì cộng dồn. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455 hàng) · sau: **9.100 hàng** (khớp 14 ngày $\times$ 650 khách hàng, đạt 100% kỳ vọng) · checksum 3 lượt ổn định: `f8d3f591f0` |

### Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Trong môi trường production thực tế, đại lượng `max` có thể bị kéo dài vô hạn bởi các trường hợp dị thường (outliers) như thiết bị mất kết nối mạng hàng tháng trời hoặc message bị kẹt trong Dead Letter Queue.
> - **Nếu chọn `max` làm lookback window:** Mọi run batch hàng ngày đều phải quét và tính toán lại dữ liệu của nhiều tháng/năm quá khứ. Chi phí compute, I/O và thời gian chạy batch sẽ tăng theo cấp số nhân (unbounded compute cost).
> - **Nếu chọn `P99` (hoặc `P99.9`):** Giới hạn được tài nguyên tính toán ở mức cố định (bounded cost) trong khi vẫn đảm bảo tự động phục hồi chính xác 99% dữ liệu về muộn trong luồng xử lý chính. Đối với 1% dữ liệu trễ ngoài P99, giải pháp chuẩn trong kỹ nghệ dữ liệu là sử dụng một quy trình reconciliation/backfill định kỳ riêng (chạy hàng tuần hoặc hàng tháng).

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Phiếu #1047: Backend đổi kiểu dữ liệu cột `priority` từ số sang chuỗi từ ngày 08-10. Pipeline không dừng nhưng mô hình phân loại dự đoán kém hẳn do 6.606 hàng bị sai/NULL và xuất hiện các giá trị dị thường. |
| **Nguyên nhân** | Schema evolution từ phía nguồn (đổi từ số 1..4 sang chuỗi `urgent`, `high`, `medium`, `low`) không tương thích với hàm `try_cast(... as integer)`. `try_cast` biến toàn bộ nhãn chữ hợp lệ thành `NULL`, đồng thời lại chấp nhận các giá trị số không hợp lệ nằm ngoài miền (`0`, `5`, `-1`). Thêm vào đó, `silver_tickets` đang để `enforced: false`, thiếu test giá trị hợp lệ, chưa có cơ chế quarantine và sắp xếp thứ tự lọc sau khi `row_number` dẫn đến nguy cơ mất cả ticket. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **1. Số hợp lệ (`1, 2, 3, 4`):** Giữ nguyên giá trị số.<br>**2. Nhãn chuỗi hợp lệ (`urgent, high, medium, low`):** Quy đổi về số tương ứng theo API contract: `urgent` $\rightarrow 1$, `high` $\rightarrow 2$, `medium` $\rightarrow 3$, `low` $\rightarrow 4$.<br>**3. Giá trị lỗi thật (`P1, P2, 0, 5, -1, unknown, '', NULL`):** Trả về `NULL` trong macro và định tuyến vào bảng `quarantine_tickets`. |
| **Cách khắc phục** | - `dbt/macros/normalize_priority.sql`: Viết khối `CASE` xử lý trọn vẹn 3 nhóm giá trị và phân loại reject reason.<br>- `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi lỗi trước khi thực hiện `row_number()` để loại bỏ bản ghi CDC hỏng mà vẫn giữ được trạng thái hợp lệ trước đó của ticket (bảo toàn đủ 12.480 ticket).<br>- `dbt/models/silver/quarantine_tickets.sql`: Đổi điều kiện thành `where {{ normalize_priority('priority_raw') }} is null`.<br>- `dbt/models/silver/schema.yml`: Bật `contract: enforced: true` và thêm tests `not_null`, `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312 hàng** (đúng 312 bản ghi CDC lỗi) · `silver_tickets` = 12.480 ticket (không bị giảm) · `silver_tickets.priority` sạch 100% · `dbt test` **11/11 pass** |

### Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao không để pipeline dừng khi gặp bản ghi lỗi?

> 1. **Nên chặn ở tầng Bronze hay Silver?**
>    - Tuyệt đối **không nên chặn/drop dữ liệu ở tầng Bronze**. Bronze là tầng lưu trữ nguyên bản (raw immutable store / source of truth). Nếu Bronze từ chối dữ liệu, dữ liệu gốc sẽ bị mất vĩnh viễn, ngăn cản việc audit, trace log và khiến đội Data Engineer không thể replay lại dữ liệu khi backend khắc phục xong sự cố.
>    - Tầng **Silver** mới là nơi thực thi Data Contract, làm sạch, chuẩn hoá và phân loại dữ liệu (routing into clean vs quarantine).
>
> 2. **Vì sao không để pipeline dừng khi gặp bản ghi lỗi?**
>    - Tỷ lệ bản ghi lỗi thường rất nhỏ (trong bài lab là 312 bản ghi lỗi trên 130.000 events và 14.300 CDC logs, chiếm < 2%). Nếu để pipeline dừng (fail DAG), toàn bộ dữ liệu hợp lệ của 98% còn lại sẽ bị nghẽn, làm sập các dịch vụ downstream quan trọng phục vụ kinh doanh (RAG index, Dashboard CSKH, Agent định tuyến).
>    - Áp dụng mô hình **Dead Letter Queue (Quarantine Table)** giúp pipeline duy trì tính sẵn sàng cao (High Availability), tiếp tục phục vụ dữ liệu sạch cho downstream, trong khi đội trực vận hành có thể chủ động theo dõi bảng `quarantine_tickets` để xử lý ngoại lệ.

---

## 4 · *(mở rộng)* Bài trong EXTRA.md

### Bài A — Tối ưu truy vấn Dashboard chậm (+5 điểm)

| | |
|---|---|
| **Bài đã làm** | **Bài A** — Tối ưu layout Parquet và truy vấn Dashboard |
| **Nguyên nhân** | Dataset `data/gold_events/` gồm 5.000 file Parquet nhỏ không được phân vùng (small-file problem), hàng sắp xếp ngẫu nhiên. Truy vấn `queries/dashboard.sql` sử dụng điều kiện `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` (non-sargable predicate) khiến DuckDB không thể tỉa phân vùng (partition pruning) hay lọc min/max metadata của row groups, buộc phải quét toàn bộ 5.000 file với công quét 5.000.000 rows scanned. |
| **Cách khắc phục** | - `tools/compact.py`: Đọc 5.000 file nhỏ, sắp xếp theo `customer_name, event_time`, phân vùng theo `event_date` (`partition_by (event_date)`), thiết lập `ROW_GROUP_SIZE 2048`, ghi ra `data/gold_events_v2`.<br>- `queries/dashboard.sql`: Trỏ sang `data/gold_events_v2/*/*.parquet` với `hive_partitioning = 1`, đổi điều kiện thành predicate sargable trực tiếp trên cột partition: `event_date = '2026-08-09'`. |
| **Bằng chứng** | - `rows scanned`: từ 5.000.000 giảm xuống **9.324** (giảm **536.3×**, vượt xa yêu cầu $\ge 10\times$).<br>- Số file Parquet: từ 5.000 file giảm xuống **14 file**.<br>- `result hash`: giữ nguyên tuyệt đối `4379e4c5d9f3`. |

---

### Bài B — Xử lý Consumer gặp sự cố giữa batch (+5 điểm)

| | |
|---|---|
| **Bài đã làm** | **Bài B** — Consumer Crash Test & Idempotent Write |
| **Nguyên nhân** | Ban đầu consumer commit offset trước khi ghi dữ liệu (ngữ nghĩa At-Most-Once). Khi tiến trình bị `kill -9` tại `maybe_crash()`, dữ liệu batch chưa được ghi nhưng offset đã tăng, dẫn đến mất dữ liệu khi restart. Nếu chỉ đảo thứ tự commit sau khi ghi (At-Least-Once) mà vẫn dùng `INSERT` thuần thì khi replay batch sẽ gây ra trùng lặp dữ liệu (duplicate rows). |
| **Cách khắc phục** | - `ingest/consumer.py`: Thêm `primary key (event_id)` vào DDL bảng `bronze_events_stream`.<br>- Trong `consume()`: Đổi thứ tự thành **Ghi dữ liệu trước (`write_batch`) $\rightarrow$ Crash test (`maybe_crash`) $\rightarrow$ Commit offset (`consumer.commit()`)** để đạt ngữ nghĩa At-Least-Once.<br>- Trong `write_batch()`: Sử dụng cú pháp ghi idempotent: `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...` để khi replay message bị trùng, dữ liệu được cập nhật trạng thái mới nhất thay vì chèn dòng mới. |
| **Bằng chứng** | Chạy `make crash-test` đạt 100% tiêu chí:<br>- Không mất bản ghi (0 hàng mất)<br>- Không trùng bản ghi (0 hàng trùng)<br>- $C == A = 20.000\text{ hàng}$<br>- `BÀI MỞ RỘNG B: ĐẠT ✓` |

#### Phân tích DO UPDATE vs DO NOTHING:
- `DO NOTHING`: Bỏ qua việc ghi khi gặp khoá chính đã tồn tại. Phù hợp cho luồng dữ liệu sự kiện bất biến (immutable append-only events).
- `DO UPDATE`: Ghi đè các trường dữ liệu bằng giá trị mới nhất (`excluded.*`). Cần thiết khi một message được replay mang theo nội dung đã được cập nhật/đính chính từ upstream hoặc khi cần cập nhật trường thời gian nhận `_ingested_at`. Lựa chọn `DO UPDATE` đảm bảo tính nhất quán dữ liệu cao nhất.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| **1** | Kiểm tra chiến lược materialization của các bảng incremental (đã có `unique_key` và `incremental_strategy` phù hợp chưa), và kiểm tra cấu hình scheduler (`catchup=False`, `max_active_runs=1`) để đảm bảo toàn bộ pipeline đạt tính idempotent khi chạy lại. |
| **2** | Đo đạc phân tích độ trễ thực tế ($P_{99}$ latency) của các nguồn dữ liệu streaming/event để thiết lập lookback window thích hợp cho mô hình incremental, tránh tình trạng bỏ sót late-arriving data. |
| **3** | Kiểm tra Data Contract ở tầng Silver (`contract: enforced: true`), các ràng buộc test miền giá trị, và thiết kế sẵn bảng cách ly (Quarantine / Dead Letter Queue) để cô lập dữ liệu lỗi mà không làm dừng cả pipeline. |
