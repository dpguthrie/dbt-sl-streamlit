# first party
from llm.schema import Query


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
                        "sql": "{{{{ Dimension('customer__customer_country') }}}} = 'United States'"
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
                "where": [{"sql": "{{{{ Dimension('customer__city') }}}} = 'Denver'"}],
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
                        "sql": "{{{{ TimeDimension('metric_time', 'MONTH') }}}} >= dateadd('month', -3, current_date)"
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
                        "sql": "{{{{ TimeDimension('metric_time', 'DAY') }}}} >= date_trunc('year', current_date)"
                    }
                ],
            }
        ),
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
                        "sql": "{{{{ TimeDimension('metric_time', 'DAY') }}}} >= date_trunc('year', current_date)"
                    }
                ],
                "orderBy": [
                    {"groupBy": {"name": "metric_time", "grain": "YEAR"}},
                    {"groupBy": {"name": "region"}},
                ],
            }
        ),
    },
]
