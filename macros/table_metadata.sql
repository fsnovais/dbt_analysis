{% macro store_metadata(results) %}
    {# Import necessary modules #}
    {% set now = modules.datetime.datetime.now(modules.pytz.utc) %}
    {% set schema_name = 'dbt_fnovais' %}
    {% set table_name = 'dbt_table_metadata' %}
    {% set metadata = [] %}

    {# Ensure the metadata storage table exists #}
    {%- if not adapter.get_relation(
            database=target.database, 
            schema=schema_name, 
            identifier=table_name
        )
    -%}
    {{log("Creating table: "~adapter.quote(schema_name~"."~table_name), info=true)}}
        {%- set query -%}
            CREATE TABLE `{{schema_name}}.{{table_name}}` (
                execution_date TIMESTAMP,
                database STRING,
                schema STRING,
                model_name STRING,
                materialization STRING,
                status STRING, 
                execution_time FLOAT64
            );
        {% endset %}
        {%- call statement(auto_begin=True) -%}
            {{query}}
        {%- endcall -%}
    {%- endif -%}

    {# Capture metadata for each model #}
    {% for result in results %}
        {% if result.node.resource_type == 'model' %}
            {% set table_metadata = {
                'execution_date': now,
                'database': result.node.database,
                'schema': result.node.schema,
                'model_name': result.node.name,
                'materialization': result.node.config.materialized,
                'status': result.status,
                'execution_time': result.execution_time
            } %}
            {% do metadata.append(table_metadata) %}
        {% endif %}
    {% endfor %}

    {# Insert metadata into the table #}
    {% if metadata | length > 0 %}
     {{log("Ingesting metadata in the table: "~adapter.quote(schema_name~"."~table_name), info=true)}}
        {% set insert_query %}
INSERT INTO `{{ schema_name }}.{{ table_name }}` (execution_date, database, schema, model_name, materialization, status, execution_time)
VALUES
        {%- for table_metadata in metadata -%}
            (
                '{{ table_metadata['execution_date'] }}',
                '{{ table_metadata['database'] }}',
                '{{ table_metadata['schema'] }}',
                '{{ table_metadata['model_name'] }}',
                '{{ table_metadata['materialization'] }}',
                '{{ table_metadata['status'] }}',
                {{ table_metadata['execution_time'] }}
            ){% if not loop.last %},{% endif %}
        {%- endfor -%};
        {% endset %}
        {%- call statement(auto_begin=True) -%}
            {{insert_query}}
        {%- endcall -%}
    {% endif %}
{% endmacro %}
