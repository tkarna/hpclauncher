"""
Interface to read user YAML config files.

Tuomas Karna 2015-09-02
"""
import yaml
from collections import OrderedDict

class OrderedDictYAMLLoader(yaml.Loader):
    """
    https://stackoverflow.com/questions/5121931/in-python-how-can-you-load-yaml-mappings-as-ordereddicts

    A YAML loader that loads mappings into ordered dictionaries.
    """

    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)

        self.add_constructor(u'tag:yaml.org,2002:map', type(self).construct_yaml_map)
        self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_yaml_map)

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructorError(None, None,
                'expected a mapping node, but found %s' % node.id, node.start_mark)

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError, exc:
                raise yaml.constructor.ConstructorError('while constructing a mapping',
                    node.start_mark, 'found unacceptable key (%s)' % exc, key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


def readYamlSpecFile(spec_file, verbose=False):
    """
    Read params from yaml spec file, sanitizes, and return as dict.
    """
    with open(spec_file, 'r') as f:
        text = yaml.load(f, Loader=OrderedDictYAMLLoader)

    # Sanitize arg input by filling in common fields not supplied with None
    # - This is to aid in formatting of diverse command strings
    if 'emailAddress' in text:
        common_args = ['accountNb', 'accountNbEntry', 'workDir', 'launcher',
                       'dependencyEntry']
        for arg in common_args:
            if arg not in text:
                if verbose:
                    print '-sanitizing: '+arg
                text[arg] = None
        # Replace None or missing parameters for non-clusters
        dev_args = ['execCmd', 'npPrefix', 'nproc']
        for arg in dev_args:
            if arg not in text:
                text[arg] = ''
            elif text[arg] is None:
                text[arg] = ''

    else:
        # Need to be added for every command to fill args.
        # Hacky - need a more elegant solution
        common_args = ['bp', 'stations', 'varList', 'level', 'dates',
                       'first', 'last', 'nthreads']
        required = ['queue', 'runTag', 'logDir']
        for k in text.keys():
            if 'task_' not in k:
                continue
            # Every task needs to know these bits
            for r in required:
                if verbose:
                    print '-adding: '+r
                text[k][r] = text[r]
            for arg in common_args:
                if arg not in text[k]:
                    if verbose:
                        print '-sanitizing: '+arg
                    text[k][arg] = None

    return text
