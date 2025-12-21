# pyspark_fabric
# 🚀 ETL Project: Lakehouse ➜ Data Warehouse using PySpark (Microsoft Fabric)

## 📌 Project Overview
![images](ETL_Pyspark.png)

Dự án này mô phỏng một **ETL pipeline thực tế** trong **Microsoft Fabric**, sử dụng **PySpark** để trích xuất dữ liệu từ **Data Lakehouse**, xử lý – chuẩn hoá dữ liệu theo các tầng xử lý, và nạp vào **Data Warehouse** phục vụ báo cáo và phân tích.


---
## Scenario
Ta sẽ vào vai trò 1 Data Engineer với 1 task nhận 1 file `AMZN.csv` trong 1 Data Lakehouse, trong file chứa các thông tin giá đóng cửa, mở cửa của cổ phiếu từ năm 1997 đến tận 2005, và công ty muốn lọc từ năm 2001 trở đi, thực hiện những phép tính đơn giản dựa trên  và load vào data warehouse trong cloud trong **Microsoft Fabric**




---

## 🏗️ Kiến trúc tổng thể

```
Data Source
   │
   ▼
Microsoft Fabric Lakehouse (Bronze)
   │  PySpark ETL
   ▼
Lakehouse Curated Data (Silver)
   │  Business Transform
   ▼
Microsoft Fabric Data Warehouse (Gold)

```
## Các bước 

---



> 💡 *Dự án được xây dựng với mục đích học tập và mô phỏng hệ thống ETL thực tế trong doanh nghiệp.*
