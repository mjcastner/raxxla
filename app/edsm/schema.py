# File metadata / schema
urls = {
    'population': 'https://www.edsm.net/dump/systemsPopulated.json.gz',
    'powerplay': 'https://www.edsm.net/dump/powerPlay.json.gz',
}
file_types = list(urls.keys())
file_types_meta = file_types.copy()
file_types_meta.append('all')
