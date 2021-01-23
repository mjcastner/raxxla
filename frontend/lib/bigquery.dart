// API reference: https://pub.dev/documentation/googleapis/latest/googleapis.bigquery.v2/googleapis.bigquery.v2-library.html
import 'dart:io';
import 'package:googleapis/bigquery/v2.dart';
import 'package:googleapis_auth/auth_io.dart';

final _credentials = new ServiceAccountCredentials.fromJson(r'''
{
  "type": "service_account",
  "project_id": "raxxla",
  "private_key_id": "3fb2494e1e0a533a76d6ef79f83af97a78ae279b",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDCNWeNZH8fu777\nj9gWZvUw/BzqZy6qyb2Uj1KAKuaQSENA1/updb3tv4qBKdXpcYNiW0E5GzCTe/pa\nMWGvys9AHiEPcaXPrEzlI8JKn+dDC9kGWQnRXPsS50LD/ueR0lT6p9+kYDFEFL5C\nuu7IlWOTriGfr6ZJYecL5ncptG4TLB/Wjd0cMOH0G+jAmLA9wWkPKJdE3BcJYuOg\nueV0NbTQbv8ruRuRrNw/C/a4B8m65j84rtlfyZWJe8aE/VZjIt/ieiaU6nUvz8+g\nv/YWApxYWw78yKC071qE+A2aoLts5Bb5QZkAmW/QxcXyzqfnD/i32LKTwPOvs8UG\nxf++48KdAgMBAAECggEAA4tuUpYr0dGfQz/VXaykZgVRUR/v/0hZBaNLX1vTwydm\nftenrWig6PwkKYZbc/wmkz7Uz8Ivc/RWgJtCdq8YyElPKNUrWxaAfXWBICk5/dnI\nrMUm7jjTEP3ClLQ22CJYJbkMVmzhCVE9mxcxW4eKfpFg5hASf2gM4QU5qzuyqfaN\n5xCNVviWgsmg2sW6lIT14pove+aJoPHg15XuoPvUL911gBzDgVTq80GdJPC88zvU\ni+tlRmw16gaki6rvee2TiwyW1/sYR6Nuh4PNeePJiPkVVpsoXxDi+6S7gjRZZQEW\nfnhlbsJ3OVYlKPhOaB4nNR9NNJXw3Seli0Aj6UorMQKBgQDhQqI5vPgoPchMBslg\nqtBfRLvNa2a6A+gLi6TKsfePHk34Lvc5PZbL6cob6GHYTK0vKMS3T97f4EjIjEyh\nauXQzz9Oliw7OWp5/n6rSdirJneYKRmHZAoadcx9PqN5JBCRUuNYQeRUK10OV3yo\nRE23ys92yNR5ifirJ/F7/fkwlQKBgQDctf6ZYSnIphEzaj+8q2byRWX1BtxxJIvn\nbY4xNn+r18zpZuyysHN1IioxA0BhOYzlvYfJSLSraKkvfFCVHEDhtlqTrEnRFHSi\n8OgVrhwC8DIVn2Syd/hAaNU2DsJ0UxEx6830c0CmjMD23Bpzr1Za59yu/l0PlQR3\nYdcnvcKf6QKBgQDB9+/bSgZiGHlORhXH4K6XKoeQ5mfJy61Xq8KWK9MhcRXwnPiT\nWJ5uLn2ztFH0wGnsju19cBBZtTbXQkCGSpdkS+GWmSezQ8iVDfkjI+6nyfL3moR6\nGkG88SzJuFNp8A04JijQCcVEWSbDP8B+4HoPxlsJTPvxQEZZk4aUn2ihVQKBgEmI\nzVG24J/8TGDP9npatQrk+ko/xfRgU8iAZM6atMDBPoFJDHWgemc9QcdgqPN7pCjr\nE7GJasBtN2kdxw9XAXryMY1f6pwhb5bWIs0OXEDSXC1+FKOteuWix96h3kG5Z1I1\nmUHnoFOdM7FLtfhzI5dYBtu63bCSWpGX5IJWI+D5AoGBAMzUoODb4N0gvAUzH+tI\nFOpZIHMY79ttekBXEsGnHASZKRMfTHEVQ9S+rjfv2CT6O3tMUevyzHaj1K67KSf9\n+nvaZBSuI/DYgrZ+tXTXmzPULozMZJzvnr5uPonJVSVhFiBlkK9aImz4Fk0/MXZo\n6g5KLJUX3O9eLgX43KMpO0bq\n-----END PRIVATE KEY-----\n",
  "client_email": "macbook-dev@raxxla.iam.gserviceaccount.com",
  "client_id": "113684288050508438227",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/macbook-dev%40raxxla.iam.gserviceaccount.com"
}
''');

const _SCOPES = const [
  'https://www.googleapis.com/auth/userinfo.profile',
  'https://www.googleapis.com/auth/cloud-platform.read-only',
];

getBigQueryTables() {
  clientViaServiceAccount(_credentials, _SCOPES).then((httpClient) async {
    var bigqueryClient = new BigqueryApi(httpClient);
    var results = await bigqueryClient.tables.list('raxxla', 'edsm');
    results.tables.forEach((element) => print(element.id));

    var query = QueryRequest();
    query.query = '''
      SELECT 
        system_id,
        relative_id,
        name,
        metadata.distance,
        metadata.type,
        metadata.sub_type,
        metadata.landable,
      FROM 
        raxxla.edsm.bodies
      WHERE
        system_id = 421384883115;
    ''';

    var test = await bigqueryClient.jobs.query(query, 'raxxla');
    test.schema.fields.forEach((element) => {print(element.name)});
    test.rows.forEach((element) => {print(element.toJson()['f'])});
  });
}
