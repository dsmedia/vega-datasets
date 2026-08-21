## `{{ resource.name }}`{% if resource.title %} {{ resource.title }}{% endif %}

{% if resource.path %}
### path
{{ resource.path }}
{% endif %}
{% if resource.description %}
### description
{{ resource.description }}
{% endif %}
{% if resource.schema %}
### schema
{% set schema_metadata = resource.schema | filter_dict(exclude=['fields']) %}
{% if schema_metadata %}
{{ schema_metadata | dict_to_markdown(level=2) }}
{% endif %}
{{ resource.schema.fields | tabulate() }}
{% endif %}
{% if resource.sources %}
### sources
{{ resource.sources | tabulate() }}
{% endif %}
{% if resource.licenses %}
### licenses
{{ resource.licenses | tabulate() }}
{% endif %}
