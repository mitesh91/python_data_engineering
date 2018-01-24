from source_config import Config

def get_credentials(dsn):

    def __get_odin_material(material_name, material_type, decode=True):

        import httplib, json
        from base64 import b64decode

        conn = httplib.HTTPConnection('localhost', 2009)
        conn.request("GET", "/query?Operation=retrieve&ContentType=JSON&material.materialName={material_name}&material.materialType={material_type}".format(material_name=material_name,material_type=material_type))
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data)
        if decode:
            material = b64decode(json_data['material']['materialData'])
        else:
            material = json_data['material']['materialData']
        return material

    def __get_encrypted_odin_material(material_name, material_type, decode=True):

        from spdacm import spda_deobfuscate_password
        key = __get_odin_material(material_name, material_type, decode)

        return spda_deobfuscate_password(key)

    datasource = Config().get(dsn)
    if datasource.type in ('redshift','oracle','s3'):
        if datasource.credential_type == 'odin':
            user, password = __get_odin_material(datasource.material, 'Principal'), __get_odin_material(datasource.material, 'Credential')
            return user, password
        elif datasource.credential_type == 'encrypted_odin':
            user, password = __get_odin_material(datasource.material, 'Principal'), __get_encrypted_odin_material(datasource.material, 'Credential')
            return user, password
        elif datasource.credential_type == 'simple':
            return datasource.user, datasource.password
    elif datasource.type == 'symmetrickey':
        if datasource.credential_type == 'odin':
            return __get_odin_material(datasource.material, 'SymmetricKey', decode=False)
        elif datasource.credential_type == 'simple':
            return datasource.symmetrickey
