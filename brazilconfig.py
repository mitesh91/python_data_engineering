"""Module for BrazilConfig functions."""

import brazilconfigparser


class BrazilConfigReader:
    """Class that retrieves BrazilConfig values specified in a file."""

    CONFIG_FILENAME = 'brazil-config/FBAIAPPythonScripts.cfg'
    KEY_FORMAT = '{domain}.{realm}.{key}'

    def __init__(self, domain, realm):
        """Constructs a BrazilConfigReader.

        Args:
            domain (str): The application's domain (devo/prod).
            realm (str): The application's realm (USAmazon, EUAmazon, JPAmazon, CNAmazon).
        """
        self.domain = domain
        self.realm = realm
        parser = brazilconfigparser.BrazilConfigParser(self.CONFIG_FILENAME)
        self.brazil_config = parser.parse()

    def get_value(self, key):
        """Returns the value associated with the specified key.

        If the key is not found, an Exception is thrown.

        Args:
            key (str): The configuration key to look up in BrazilConfig.

        Returns:
            The value associated with the key.
        """
        key = self.KEY_FORMAT.format(
            domain=self.domain, realm=self.realm, key=key)

        if key not in self.brazil_config:
            raise Exception(
                'Key: <%s> is not defined in BrazilConfig'.format(key))
        else:
            value = self.brazil_config[key]
            return value
