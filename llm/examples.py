# first party
from schema import Query


EXAMPLES = [
    {
        "metrics": "total_revenue, total_expense, total_profit, monthly_customers",
        "dimensions": "metric_time, customer__customer_region, customer__customer_country",
        "question": "What is total revenue by month for customers in the United States?",
        "result": Query.model_validate(
            {
                "metrics": [{"name": "total_revenue"}],
                "groupBy": [{"name": "metric_time", "grain": "MONTH"}],
                "where": [
                    {
                        "sql": "{{ Dimension('customer__customer_country') }} ilike 'United States'"
                    }
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "total_cost, total_sales, avg_sales_per_cust, avg_cost_per_cust",
        "dimensions": "metric_time, order_date, customer__country, customer__region, customer__city, customer__is_active",
        "question": "What is the average revenue and cost per customer by quarter for customers in Denver ordered by average revenue descending?",
        "result": Query.model_validate(
            {
                "metrics": [
                    {"name": "avg_sales_per_cust"},
                    {"name": "avg_cost_per_cust"},
                ],
                "groupBy": [{"name": "metric_time", "grain": "QUARTER"}],
                "where": [{"sql": "{{ Dimension('customer__city') }} ilike 'Denver'"}],
                "orderBy": [
                    {"metric": {"name": "avg_sales_per_cust"}, "descending": True}
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "sales, expenses, profit, sales_ttm, expenses_ttm, profit_ttm, sales_yoy, expenses_yoy, profit_yoy",
        "dimensions": "department, salesperson, cost_center, metric_time, product__product_category, product__product_subcategory, product__product_name",
        "question": "What is the total sales, expenses, and profit for the last 3 months by product category?",
        "result": Query.model_validate(
            {
                "metrics": [
                    {"name": "sales"},
                    {"name": "expenses"},
                    {"name": "profit"},
                ],
                "groupBy": [
                    {"name": "metric_time", "grain": "MONTH"},
                    {"name": "product__product_category"},
                ],
                "where": [
                    {
                        "sql": "{{ TimeDimension('metric_time', 'MONTH') }} >= dateadd('month', -3, current_date)"
                    }
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "revenue, costs, profit",
        "dimensions": "department, salesperson, cost_center, metric_time, product__product_category",
        "question": "What is the total revenue by department this year?",
        "result": Query.model_validate(
            {
                "metrics": [{"name": "revenue"}],
                "groupBy": [{"name": "department"}],
                "where": [
                    {
                        "sql": "{{ TimeDimension('metric_time', 'DAY') }} >= date_trunc('year', current_date)"
                    }
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "total_revenue, total_expense, total_customers, monthly_customers, weekly_customers, daily_customers",
        "dimensions": "department, salesperson, cost_center, metric_time, product__product_category, customer__region, customer__balance_segment",
        "question": "What is total revenue by region and balance segment year-to-date over time ordered by time and region?",
        "result": Query.model_validate(
            {
                "metrics": [{"name": "revenue"}],
                "groupBy": [
                    {"name": "customer__region"},
                    {"name": "customer__balance_segment"},
                    {"name": "metric_time", "grain": "YEAR"},
                ],
                "where": [
                    {
                        "sql": "{{ TimeDimension('metric_time', 'DAY') }} >= date_trunc('year', current_date)"
                    }
                ],
                "orderBy": [
                    {"groupBy": {"name": "metric_time", "grain": "YEAR"}},
                    {"groupBy": {"name": "region"}},
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "total_revenue, total_expense, total_profit, total_customers, monthly_customers, weekly_customers, daily_customers",
        "dimensions": "department, salesperson, cost_center, metric_time, product__product_category, customer__region, customer__balance_segment",
        "question": "Can you give me revenue, expense, and profit in 2023?",
        "result": Query.model_validate(
            {
                "metrics": [
                    {"name": "total_revenue"},
                    {"name": "total_expense"},
                    {"name": "total_profit"},
                ],
                "where": [
                    {"sql": "year({{ TimeDimension('metric_time', 'DAY') }}) = 2023"}
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "total_revenue, total_expense, total_profit, total_customers, monthly_customers, weekly_customers, daily_customers",
        "dimensions": "department, salesperson, cost_center, metric_time, product__product_category, customer__region, customer__balance_segment",
        "question": "Can you give me the top 10 sales people by revenue in September 2023?",
        "result": Query.model_validate(
            {
                "metrics": [
                    {"name": "total_revenue"},
                ],
                "groupBy": [
                    {"name": "salesperson"},
                ],
                "where": [
                    {
                        "sql": "{{ TimeDimension('metric_time', 'DAY') }} between '2023-09-01' and '2023-09-30'"
                    }
                ],
                "orderBy": [
                    {"metric": {"name": "total_revenue"}, "descending": True},
                ],
                "limit": 10,
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "total_revenue, total_expense, total_profit, total_customers, monthly_customers, weekly_customers, daily_customers",
        "dimensions": "department, salesperson, cost_center, metric_time, product__product_category, customer__region, customer__balance_segment",
        "question": "Can you give me revenue by salesperson in the first quarter of 2023 where product category is either cars or motorcycles?",
        "result": Query.model_validate(
            {
                "metrics": [
                    {"name": "total_revenue"},
                ],
                "groupBy": [
                    {"name": "salesperson"},
                ],
                "where": [
                    {
                        "sql": "{{ TimeDimension('metric_time', 'DAY') }} between '2023-01-01' and '2023-03-31'"
                    },
                    {
                        "sql": "{{ Dimension('product__product_category') }} ilike any ('cars', 'motorcycles')"
                    },
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "campaigns, impressions, clicks, conversions, roi, run_rate, arr, mrr, ltv, cac",
        "dimensions": "customer__geography__country, customer__market_segment, customer__industry, customer__sector, metric_time",
        "question": "What is the annual recurring revenue and customer acquisition cost by month and country in the United States?",
        "result": Query.model_validate(
            {
                "metrics": [
                    {"name": "arr"},
                    {"name": "cac"},
                ],
                "groupBy": [
                    {"name": "customer__geography__country"},
                    {"name": "metric_time", "grain": "MONTH"},
                ],
                "where": [
                    {
                        "sql": "{{ Dimension('geography__country, entity_path=['customer']) }} ilike 'United States'"
                    },
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "campaigns, impressions, clicks, conversions, roi, run_rate, arr, mrr, ltv, cac",
        "dimensions": "customer__geography__country, customer__market_segment, customer__industry, customer__sector, metric_time, close_date",
        "question": "What are the 5 worst performing sectors by arr last month in the enterprise segment?",
        "result": Query.model_validate(
            {
                "metrics": [
                    {"name": "arr"},
                ],
                "groupBy": [
                    {"name": "customer__sector"},
                ],
                "where": [
                    {
                        "sql": "{{ Dimension('customer__market_segment') }} = 'enterprise'"
                    },
                    {
                        "sql": "date_trunc('month', {{ TimeDimension('metric_time', 'DAY') }}) = date_trunc('month', dateadd('month', -1, current_date))"
                    },
                ],
                "orderBy": [
                    {"metric": {"name": "arr"}},
                ],
                "limit": 5,
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
    {
        "metrics": "total_revenue, total_expense, total_profit, total_customers, monthly_customers, weekly_customers, daily_customers",
        "dimensions": "department, salesperson, cost_center, metric_time, product__product_category, customer__region, customer__balance_segment",
        "question": "What is total profit in 2022 by quarter?",
        "result": Query.model_validate(
            {
                "metrics": [
                    {"name": "total_revenue"},
                ],
                "groupBy": [
                    {"name": "metric_time", "grain": "QUARTER"},
                ],
                "where": [
                    {
                        "sql": "{{ TimeDimension('metric_time', 'DAY') }} between '2022-01-01' and '2022-12-31'"
                    },
                ],
            }
        )
        .model_dump_json()
        .replace("{", "{{")
        .replace("}", "}}"),
    },
]
