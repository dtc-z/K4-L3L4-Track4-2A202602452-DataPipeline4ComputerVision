# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Vũ Hải Minh` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602452` |
| GitHub | `dtc-z` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `0739495c79d9`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `716 / 158 / 145` (tổng `1.019`)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A (`drive_a`, 3.021 dòng) dùng `ts_utc` dạng ISO UTC và `speed_mps` (m/s); nguồn B (`drive_b`, 3.234 dòng) dùng `timestamp_ms` dạng epoch milliseconds và `spd_kmh` (km/h). Schema cũng đổi tên nhiều trường tương đương: `heading_deg`/`hdg`, `lat`–`lon`/`latitude`–`longitude`, `alt_m`/`altitude`, `acc_x`–`acc_y`/`ax`–`ay`, `gas`/`throttle_pct`, `brake`/`brake_pct`, `steer`/`steering`; B còn biểu diễn bàn đạp theo phần trăm.

Bronze giữ hai bảng riêng (`raw_telemetry_a`, `raw_telemetry_b`) để không làm mất schema và giá trị nguồn khi hai producer chưa thống nhất. Các trường lấy từ CSV, kể cả timestamp, số đo và ô trống, được giữ nguyên dưới dạng chuỗi; chỉ metadata ingest như `_row_no` và `_ingested_at` có kiểu riêng. Việc chuẩn hoá tên cột, đơn vị và thời gian để Silver xử lý sau, có thể kiểm tra lại từ dữ liệu gốc. Nạp lặp lại, file được nhận diện theo SHA-256 nên báo `already present`; bảng `bronze.raw_telemetry_a` vẫn chỉ có một commit Delta. Video gốc nằm ở `bronze/video/<drive>/raw.mp4`, còn `bronze.video_files` lưu con trỏ cùng SHA-256.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging đưa A từ `ts_utc` ISO UTC và `speed_mps` sang schema chung; B đổi `timestamp_ms` sang timestamp, trừ độ lệch đồng hồ 1.500 ms, đổi tên các trường và chuyển `spd_kmh` sang m/s bằng cách chia 3,6. Thời điểm đầu B sau hiệu chỉnh là `2025-10-08 14:30:00.009 UTC`, chỉ lệch 9 ms so với video bắt đầu lúc `14:30:00 UTC`.

| Lý do quarantine | Drive A | Drive B |
|---|---:|---:|
| Trùng (`duplicate`) | 20 | 20 |
| Thiếu tốc độ (`speed_missing`) | 30 | 30 |
| Tốc độ ngoài khoảng (`speed_out_of_range`) | 3 | 3 |

Mỗi drive có 53 dòng bị cách ly; còn lại A 2.968 và B 3.181 dòng sạch. Tổng sạch + quarantine khớp số dòng nhận vào (3.021 và 3.234). Giữ lỗi trong `silver_telemetry_rejects` kèm lý do giúp truy vết chất lượng nguồn và tránh mất dữ liệu âm thầm.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` đặt `severity: warn` vì khoảng trống cảm biến cần được báo để theo dõi nhưng không nhất thiết chặn dữ liệu còn hợp lệ. `in_range` để mặc định `error` vì tốc độ ngoài giới hạn là dữ liệu vô lý không nên đi tiếp. Trong `break_gate.json`, nới giới hạn lên 1.000 m/s khiến test `in_range_silver_telemetry_speed_mps__70__0` bắt được 6 dòng: kết quả là `PASS=5 WARN=1 ERROR=1`. Khôi phục giới hạn gốc cho `PASS=6 WARN=1 ERROR=0`, cho thấy quality gate chặn lỗi nghiêm trọng trong khi vẫn giữ cảnh báo về mất tín hiệu.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Với `trip-02134`, bảng có 1.246 dòng trước và sau hai lần chạy lại; dấu vân tay vẫn là `6c7480dfde7e`. Trip này có 60 khung: ghi `append` hai lần tạo 120 dòng, tức nhân đôi. Pipeline ghi đè có predicate theo `trip_id`, thay dữ liệu của trip đó trong một commit Delta và giữ nguyên các trip khác. Nếu tách thành xoá rồi ghi, lỗi giữa hai thao tác có thể để trip bị mất; giao dịch ghi đè giữ phiên bản cũ cho đến khi thay thế hoàn tất.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có độ sáng dưới ngưỡng `dark_luma` nên bị loại khỏi tập Gold; khung tối nằm trong khoảng `dark_luma`–`dark_soft`, được gắn cờ nhưng giữ lại vì cảnh tối vẫn có thể hữu ích. Kết quả có 6/4 khung đen ở A/B và 4/61 khung tối được giữ. Mình đồng ý với cách này. Chọn dHash ngưỡng 2: ngưỡng 0 bỏ sót các khung gần giống nhưng không tuyệt đối giống; ngưỡng 6 tăng số khung bị gắn trùng và có nguy cơ loại nhầm cảnh khác nhau. Xe chạy chậm hoặc camera đứng hình tạo nhiều khung gần nhau; ở tốc độ cao cảnh đổi nhanh hơn giữa mẫu hình nên cùng ngưỡng có thể bắt ít trùng hơn.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
