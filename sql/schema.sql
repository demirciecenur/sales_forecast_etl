-- Dimension Tables

CREATE TABLE IF NOT EXISTS dim_material ( material_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                          material_number TEXT NOT NULL UNIQUE);


CREATE TABLE IF NOT EXISTS dim_time ( time_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                  period TEXT NOT NULL UNIQUE,
                                                                                       year INTEGER NOT NULL);


CREATE TABLE IF NOT EXISTS dim_region ( region_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                      region_code TEXT NOT NULL UNIQUE,
                                                                                                region_description TEXT NOT NULL);

-- Add indexes for better performance

CREATE INDEX idx_region_code ON dim_region(region_code);

-- Fact Tables

CREATE TABLE IF NOT EXISTS fact_sales ( sales_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                     material_id INTEGER NOT NULL,
                                                                                         time_id INTEGER NOT NULL,
                                                                                                         region_code TEXT NOT NULL,
                                                                                                                          gross_sales DECIMAL(18,2) NOT NULL,
                                                                                                                                                    net_sales DECIMAL(18,2) NOT NULL,
                                       FOREIGN KEY (material_id) REFERENCES dim_material(material_id),
                                       FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
                                       FOREIGN KEY (region_code) REFERENCES dim_region(region_code));


CREATE TABLE IF NOT EXISTS fact_forecast ( forecast_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                           material_id INTEGER NOT NULL,
                                                                                               time_id INTEGER NOT NULL,
                                                                                                               forecast_value DECIMAL(18,2) NOT NULL,
                                          FOREIGN KEY (material_id) REFERENCES dim_material(material_id),
                                          FOREIGN KEY (time_id) REFERENCES dim_time(time_id));

-- Indexes

CREATE INDEX idx_sales_material ON fact_sales(material_id);


CREATE INDEX idx_sales_time ON fact_sales(time_id);


CREATE INDEX idx_sales_region ON fact_sales(region_code);


CREATE INDEX idx_forecast_material ON fact_forecast(material_id);


CREATE INDEX idx_forecast_time ON fact_forecast(time_id);

-- Views

CREATE VIEW vw_sales_vs_forecast AS
SELECT m.material_number,
       t.year,
       t.period,
       CASE s.region_code
           WHEN '1' THEN 'EMEA'
           WHEN '2' THEN 'Americas'
           WHEN '4' THEN 'Asia Pacific'
       END as region_name,
       s.gross_sales,
       s.net_sales,
       f.forecast_value,
       ROUND((s.net_sales - f.forecast_value) / f.forecast_value * 100, 2) as variance_percentage
FROM fact_sales s
JOIN dim_material m ON s.material_id = m.material_id
JOIN dim_time t ON s.time_id = t.time_id
LEFT JOIN fact_forecast f ON s.material_id = f.material_id
AND s.time_id = f.time_id;