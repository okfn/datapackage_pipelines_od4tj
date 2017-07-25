import re
import logging
from datapackage_pipelines.wrapper import process


NUMERIC_FIELDS = [
    "turnover",
    "profit_before_tax",
    "corporate_tax_paid",
    "full_time_equivalents",
    "deferred_tax",
    "subsidies_received"
]


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    for field in NUMERIC_FIELDS:
        value = row.get(field)
        if value is None:
            continue

        original_value = value

        value = _convert_parenthesis_as_negative(value)

        value = _remove_null_values(value)

        if value:
            try:
                value = _convert_to_number(value,
                                           parameters.get('group_char'),
                                           parameters.get('decimal_char'))
            except (ValueError, AttributeError):
                raise
                value = None
            else:
                value = _apply_factor(value, parameters.get('factor'))

        logging.warning('Processing "{}" with value "{}".Altering to "{}"'
                        .format(field, original_value, value))

        row[field] = value

    return row


def _is_number_inside_parenthesis(value):
    if isinstance(value, str):
        return re.fullmatch(
            # r'^\s*\(?\s*[0-9,.]+\s*\)?\s*$',
            r'^\s*\(\s*[0-9,.]+\s*\)\s*$',
            value
        )


def _convert_parenthesis_as_negative(value):
    if _is_number_inside_parenthesis(value):
        value = '-{}'.format(value.strip('( )'))
    return value


def _remove_null_values(value):
    if isinstance(value, str):
        if value.strip() in (
            '-',
            '',
        ):
            value = None
    return value


def _convert_to_number(value, group_char=',', decimal_char='.'):
    if isinstance(value, str):
        value = value\
            .replace(group_char or ',', '')\
            .replace(decimal_char or '.', '.')

    numeric_value = float(value)

    if numeric_value.is_integer():
        numeric_value = int(numeric_value)

    return numeric_value


def _apply_factor(value, factor):
    if not isinstance(factor, int):
        raise KeyError()
    value *= factor
    return value


def modify_datapackage(dp, *_):
    for resource in dp['resources']:
        for field in resource['schema']['fields']:
            if field['name'] in NUMERIC_FIELDS:
                field['type'] = 'number'
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
