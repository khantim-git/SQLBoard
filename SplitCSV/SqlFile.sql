-- Create text widgets for schema and table names
CREATE WIDGET TEXT schema_name DEFAULT 'default_schema' COMMENT 'Enter Schema Name';
CREATE WIDGET TEXT table_name DEFAULT 'AAA_BBB' COMMENT 'Enter Table Name';


table_name = "database_name.table_name"  # Thay bằng tên bảng của bạn

# Lấy thông tin phân vùng của bảng
df = spark.sql(f"DESCRIBE FORMATTED {table_name}")

# Kiểm tra xem bảng có trường 'Partition' không
partitions = df.filter(df.col("col_name") == "Partition Columns")

if partitions.count() > 0:
    print(f"{table_name} là bảng phân vùng.")
else:
    print(f"{table_name} không phải là bảng phân vùng.")
