-- Data Quality Macros (shared for Postgres and DuckDB)

-- Not Null Test Macro
dbt_test_not_null = """
{% macro test_not_null(model, column_name) %}
    select * from {{ model }} where {{ column_name }} is null
{% endmacro %}
"""

-- Unique Test Macro
dbt_test_unique = """
{% macro test_unique(model, column_name) %}
    select {{ column_name }}, count(*)
    from {{ model }}
    group by {{ column_name }}
    having count(*) > 1
{% endmacro %}
"""

-- Accepted Values Test Macro
dbt_test_accepted_values = """
{% macro test_accepted_values(model, column_name, accepted_values) %}
    select * from {{ model }}
    where {{ column_name }} not in (
        {% for value in accepted_values %}
            '{{ value }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
{% endmacro %}
"""

-- Cross-Table Consistency Macro (e.g., foreign key check)
dbt_test_cross_table_consistency = """
{% macro test_cross_table_consistency(model, column_name, ref_model, ref_column) %}
    select a.*
    from {{ model }} a
    left join {{ ref_model }} b
      on a.{{ column_name }} = b.{{ ref_column }}
    where b.{{ ref_column }} is null
{% endmacro %}
"""

-- Usage: see dbt_common/tests/data_quality_tests.sql for examples
