-- q3_price_volume_comparison_inflation.sql
-- Purpose: Compare Q3 2024 vs Q3 2025 supply prices, spend, and quantity,
-- adjusted for inflation.

-- PARAMETERS
-- @facility_name = 'Your Health System'
-- @inflation_rate = 0.03   -- 3% annual inflation
-- @start_date = '2024-07-01'
-- @end_date   = '2025-09-30'

WITH
-- 1️⃣ Monthly aggregation of SKU-level data
monthly AS (
  SELECT
    SKU,
    ProductDescription,
    CategoryName,
    DATE_TRUNC(DATE(PurchaseDate), MONTH) AS year_month,
    AVG(
      CASE
        WHEN UnitOfMeasureQuantity > 0
          THEN UnitOfMeasurePrice / UnitOfMeasureQuantity
        ELSE PriceEach
      END
    ) AS avg_unit_price,
    SUM(LineAmount)     AS total_spend,
    SUM(PurchaseQuantity) AS total_quantity
  FROM `your_project.your_dataset.purchase_table`
  WHERE
    UPPER(COALESCE(ContractType, '')) = 'ON CONTRACT'
    AND UnitOfMeasureQuantity > 0
    AND FacilityName = 'Your Health System'
    AND DATE(PurchaseDate) BETWEEN DATE('2024-07-01') AND DATE('2025-09-30')
  GROUP BY 1,2,3,4
  HAVING SUM(PurchaseQuantity) > 0
),

-- 2️⃣ Compute interquartile range (IQR) for outlier filtering
ranked_monthly AS (
  SELECT
    m.*,
    PERCENTILE_APPROX(m.avg_unit_price, 0.25) OVER (PARTITION BY m.SKU) AS q1,
    PERCENTILE_APPROX(m.avg_unit_price, 0.75) OVER (PARTITION BY m.SKU) AS q3
  FROM monthly m
),

monthly_prices_clean AS (
  SELECT
    SKU,
    ProductDescription,
    CategoryName,
    year_month,
    avg_unit_price,
    total_spend,
    total_quantity
  FROM (
    SELECT *,
      SAFE_SUBTRACT(q3, q1) AS iqr
    FROM ranked_monthly
  )
  WHERE
    (iqr > 0 AND avg_unit_price BETWEEN (q1 - 1.5 * iqr) AND (q3 + 1.5 * iqr))
    OR (iqr = 0 AND avg_unit_price = q1)
),

-- 3️⃣ Aggregate to quarterly values (focus on Q3)
quarterly_agg AS (
  SELECT
    SKU,
    ProductDescription,
    CategoryName,
    CASE
      WHEN year_month BETWEEN DATE('2024-07-01') AND DATE('2024-09-30') THEN 'Q3_2024'
      WHEN year_month BETWEEN DATE('2025-07-01') AND DATE('2025-09-30') THEN 'Q3_2025'
    END AS quarter_flag,
    AVG(avg_unit_price) AS avg_unit_price_quarter,
    SUM(total_spend)    AS total_spend_quarter,
    SUM(total_quantity) AS total_quantity_quarter
  FROM monthly_prices_clean
  WHERE year_month BETWEEN DATE('2024-07-01') AND DATE('2025-09-30')
  GROUP BY 1,2,3,4
),

-- 4️⃣ Get last known price before July 2024
last_price_2024 AS (
  SELECT
    SKU,
    ProductDescription,
    CategoryName,
    avg_unit_price AS last_price_before_2024
  FROM (
    SELECT
      SKU,
      ProductDescription,
      CategoryName,
      avg_unit_price,
      ROW_NUMBER() OVER (
        PARTITION BY SKU, ProductDescription, CategoryName
        ORDER BY year_month DESC
      ) AS rn
    FROM monthly_prices_clean
    WHERE year_month < DATE('2024-07-01')
  )
  WHERE rn = 1
),

-- 5️⃣ Compare Q3 2024 vs Q3 2025 with inflation adjustment
q3_comparison AS (
  SELECT
    q.SKU,
    q.ProductDescription,
    q.CategoryName,

    -- Baseline and current quarter prices
    COALESCE(
      MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN avg_unit_price_quarter END),
      lp.last_price_before_2024
    ) AS avg_price_2024,
    MAX(CASE WHEN quarter_flag = 'Q3_2025' THEN avg_unit_price_quarter END) AS avg_price_2025,

    -- Totals
    MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN total_spend_quarter END) AS spend_2024,
    MAX(CASE WHEN quarter_flag = 'Q3_2025' THEN total_spend_quarter END) AS spend_2025,
    MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN total_quantity_quarter END) AS quantity_2024,
    MAX(CASE WHEN quarter_flag = 'Q3_2025' THEN total_quantity_quarter END) AS quantity_2025,

    -- Nominal percent change
    SAFE_DIVIDE(
      MAX(CASE WHEN quarter_flag = 'Q3_2025' THEN avg_unit_price_quarter END) -
      COALESCE(
        MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN avg_unit_price_quarter END),
        lp.last_price_before_2024
      ),
      COALESCE(
        MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN avg_unit_price_quarter END),
        lp.last_price_before_2024
      )
    ) * 100 AS pct_change_price_nominal,

    SAFE_DIVIDE(
      MAX(CASE WHEN quarter_flag = 'Q3_2025' THEN total_spend_quarter END) -
      MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN total_spend_quarter END),
      MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN total_spend_quarter END)
    ) * 100 AS pct_change_spend,

    SAFE_DIVIDE(
      MAX(CASE WHEN quarter_flag = 'Q3_2025' THEN total_quantity_quarter END) -
      MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN total_quantity_quarter END),
      MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN total_quantity_quarter END)
    ) * 100 AS pct_change_quantity,

    -- 6️⃣ Inflation adjustment (3% annual)
    -- Assume 1 year between Q3 2024 and Q3 2025
    COALESCE(
      MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN avg_unit_price_quarter END),
      lp.last_price_before_2024
    ) * (1 + 0.03) AS inflation_adjusted_price_2025,

    -- Real (inflation-adjusted) percent change
    SAFE_DIVIDE(
      MAX(CASE WHEN quarter_flag = 'Q3_2025' THEN avg_unit_price_quarter END) -
      (
        COALESCE(
          MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN avg_unit_price_quarter END),
          lp.last_price_before_2024
        ) * (1 + 0.03)
      ),
      COALESCE(
        MAX(CASE WHEN quarter_flag = 'Q3_2024' THEN avg_unit_price_quarter END),
        lp.last_price_before_2024
      ) * (1 + 0.03)
    ) * 100 AS inflation_adj_pct_change_price

  FROM quarterly_agg q
  LEFT JOIN last_price_2024 lp
    USING (SKU, ProductDescription, CategoryName)
  GROUP BY 1,2,3, lp.last_price_before_2024
)

SELECT *
FROM q3_comparison
ORDER BY inflation_adj_pct_change_price ASC;
