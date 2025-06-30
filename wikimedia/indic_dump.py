indian_lang_codes = [
    "en", # English
    "as",  # Assamese
    "bn",  # Bengali
    "gu",  # Gujarati
    "hi",  # Hindi
    "kn",  # Kannada
    "ks",  # Kashmiri
    "ml",  # Malayalam
    "mr",  # Marathi
    "ne",  # Nepali
    "or",  # Odia
    "pa",  # Punjabi
    "sa",  # Sanskrit
    "sd",  # Sindhi
    "ta",  # Tamil
    "te",  # Telugu
    "ur",  # Urdu
    "mai", # Maithili
]

categorys = ["Wiki",
"wiktionary",
"Wikiquote",
"Wikibooks",
"Wikisource",
"Wikiversity",
"Wikidata",
"Wikifunctions",
"MediaWiki",
"Wikivoyage",
"Wikinews"]


with open('./a_tags.txt', 'r') as f:
    tags = [d.replace('\n', '').replace(' ', '').strip() for d in f.readlines()]
    
dump_l = set()
not_f = dict()
for category in categorys:
    category = category.lower()
    for lang in indian_lang_codes:
        collect = lang.lower()+category
        if collect in tags:
            dump_l.add(collect)
        elif category.lower() :
            not_f[category] = not_f.get(category, [])
            not_f[category].append(lang)

extra = ['specieswiki', 'wikidatawiki']

dump_l.add('specieswiki')
dump_l.add('wikidatawiki')

with open('indic_wikimedia.txt', 'w') as f:
    f.write('\n'.join(dump_l))