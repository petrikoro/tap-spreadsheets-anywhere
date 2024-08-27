import csv
import re
import logging

LOGGER = logging.getLogger(__name__)

def generator_wrapper(reader):
    for row in reader:
        to_return = {}
        for key, value in row.items():
            if key is None:
                key = '_smart_extra'

            formatted_key = key

            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", '', formatted_key)

            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", '_', formatted_key)
            to_return[formatted_key.lower()] = value
        yield to_return

def filter_nul_lines(reader):
    """Generator that yields lines from the reader, skipping those with NUL."""
    for line in reader:
        if '\0' in line:
            LOGGER.warning("Skipping line with NUL character: %r", line)
            continue
        yield line

def get_row_iterator(table_spec, reader):
    field_names = None
    if 'field_names' in table_spec:
        field_names = table_spec['field_names']

    dialect = 'excel'
    if 'delimiter' not in table_spec or table_spec['delimiter'] == 'detect':
        try:
            # Use filter_nul_lines to wrap the reader for sniffing
            first_line = reader.readline()
            if '\0' in first_line:
                raise ValueError("First line of the file contains NUL character")
            dialect = csv.Sniffer().sniff(first_line, delimiters=[',', '\t', ';', ' ', ':', '|', ' '])
            if reader.seekable():
                reader.seek(0)
        except Exception as err:
            raise ValueError("Unable to sniff a delimiter: " + str(err))
    else:
        custom_delimiter = table_spec.get('delimiter', ',')
        custom_quotechar = table_spec.get('quotechar', '"')
        if custom_delimiter != ',' or custom_quotechar != '"':
            class custom_dialect(csv.excel):
                delimiter = custom_delimiter
                quotechar = custom_quotechar
            dialect = 'custom_dialect'
            csv.register_dialect(dialect, custom_dialect)

    # Wrap the reader with the NUL-filtering generator
    filtered_reader = filter_nul_lines(reader)
    reader = csv.DictReader(filtered_reader, fieldnames=field_names, dialect=dialect)
    return generator_wrapper(reader)
