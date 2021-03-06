# PyBabe

## tests

### unit

```bash
$ nosetests -sv tests/unit
```

### services

Need to run on local machine
- mongo


Need also a correct config file `.pybabe.cfg`

```ini
[kontagent]
KT_APPID=
KT_USER=
KT_PASS=
KT_FILECACHE=/tmp/kontagent-cache
[s3]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
cache=1
[smtp]
server=smtp.googlemail.com
port=587
login=
password=
author=
tls=True
[geoip]
GEOIP_FILE=/path/to/GeoLiteCity.dat
[appfigures]
login=
password=
```

And an environment variable pointed to a credential file for Google Cloud. See https://developers.google.com/identity/protocols/application-default-credentials for more informations.

```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/google-cloud-credential.json
```


then :

```bash
$ nosetests -sv tests/services
```