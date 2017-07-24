import re
import logging
from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    numeric_fields = [
        field for field in spec['schema']['fields']
        if field.get('type') == 'number'
    ]
    for field in numeric_fields:
        name = field['name']
        value = row[name]

        original_value = value

        value = _convert_parenthesis_as_negative(value)

        value = _remove_null_values(value)

        if value:
            try:
                value = _convert_to_number(value, field.get('groupChar'))
            except (ValueError, AttributeError):
                value = None
            else:
                value = _apply_factor(value, field.get('factor'))

        logging.warn('Processing "{}" with value "{}".Altering to "{}"'.format(
            name,
            original_value,
            value
        ))

        row[name] = value

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


def _convert_to_number(value, group_char=','):
    if isinstance(value, str):
        value = value.replace(group_char or ',', '').replace(',', '.')

    numeric_value = float(value)

    if numeric_value.is_integer():
        numeric_value = int(numeric_value)

    return numeric_value


def _apply_factor(value, factor):
    if value and factor:
        multiplier = {
            '1m': 1000000,
            '1k': 1000,
            '1':  1,
        }[factor]
        value *= multiplier

    return value


process(process_row=process_row)
