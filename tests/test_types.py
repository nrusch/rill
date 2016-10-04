from collections import OrderedDict
from tests.components import Company, PassthruPerson
from rill.jsonschematics import to_jsonschema


def test_schematics_port():
    spec = PassthruPerson.get_spec()
    inports = spec['inPorts']

    assert {
        'required': False,
        'addressable': False,
        'static': False,
        'type': 'bang',
        'id': 'wait',
        'description': ''
    } in inports
    assert {
        'required': False,
        'static': False,
        'schema': {
            'required': ['name'],
            'type': 'object',
            'properties': OrderedDict([
                ('name', {'type': 'string'}),
                ('age', {
                    'default': 0,
                    'type': 'int',
                    'minimum': 0,
                    'maximum': 200
                }),
                ('favorite_color', {
                    'enum': ['cyan', 'magenta', 'chartreuse'],
                    'type': 'string'
                }),
                ('married', {'type': 'boolean'}),
                ('phone_number', {
                    'type': 'string',
                    'maxLength': 8,
                    'minLength': 8,
                    'pattern': r'\d{3}-\d{4}'
                })
            ]),
            'title': 'Person'
        },
        'addressable': False,
        'id': 'IN',
        'description': ''
    } in inports


def test_to_jsonschema():
    expected = {
        'type': 'object',
        'properties': OrderedDict([
            ('ceo', {
                'required': ['name'],
                'type': 'object',
                'properties': OrderedDict([
                    ('name', {
                        'type': 'string',
                    }),
                    ('age', {
                        'type': 'int',
                        'default': 0,
                        'minimum': 0,
                        'maximum': 200,
                    }),
                    ('favorite_color', {
                        'type': 'string',
                        'enum': ['cyan', 'magenta', 'chartreuse'],
                    }),
                    ('married', {'type': 'boolean'}),
                    ('phone_number', {
                        'type': 'string',
                        'pattern': r'\d{3}-\d{4}',
                        'minLength': 8,
                        'maxLength': 8
                    })
                ]),
                'title': 'Person'
            }),
            ('address', {
                'type': 'string',
            }),
            ('employees', {
                'type': 'array',
                'title': 'Person Array',
                'items': {
                    'required': ['name'],
                    'type': 'object',
                    'properties': OrderedDict([
                        ('name', {
                            'type': 'string',
                        }),
                        ('age', {
                            'type': 'int',
                            'default': 0,
                            'minimum': 0,
                            'maximum': 200,
                        }),
                        ('favorite_color', {
                            'type': 'string',
                            'enum': ['cyan', 'magenta', 'chartreuse'],
                        }),
                        ('married', {'type': 'boolean'}),
                        ('phone_number', {
                            'type': 'string',
                            'pattern': r'\d{3}-\d{4}',
                            'maxLength': 8,
                            'minLength': 8,
                        })
                    ]),
                    'title': 'Person'
                }
            })
        ]),
        'title': 'Company'
    }

    schema = to_jsonschema(Company)
    assert schema == expected
