{% test matches_regex(model, column_name, pattern) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and not regexp_contains({{ column_name }}, r'{{ pattern }}')

{% endtest %}