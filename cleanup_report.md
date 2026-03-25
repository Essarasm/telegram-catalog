# Client Data Cleanup Report

**Date:** 2026-03-25

## Summary

| Metric | Count |
|--------|-------|
| Original rows | 2321 |
| Unique phones | 2039 |
| After cleanup | 2039 |
| Duplicates merged | 282 |
| Removed (empty) | 0 |
| Suspicious (review needed) | 23 |
| Short phone numbers | 24 |

## Merge Strategy

For each duplicate phone number:
- **Name:** kept the longest/most complete name
- **Location:** kept the most specific (longest) location, preferring Contacts source
- **Source:** combined all sources (e.g. "Contacts, Jomboy")
- **New columns added:** `client_id_1c` (empty), `company_name` (empty)

## Duplicate Categories

Most duplicates (260+) were simple cross-source matches:
- Same person in Contacts + Jomboy/Bulungur/Usto lists
- Contacts had more specific locations (e.g. "Bulung'ur tuman, Mingchinor" vs just "Bulungur")
- Some exact duplicates within same source

## Short Phone Numbers

These phones have fewer than 9 digits and may not match correctly:

- `2120110`: Furkat, Samarqand shahar, Gorgaz
- `2210853`: Furkat, Samarqand shahar, Marxabo
- `2249464`: Furkat, Samarqand shahar
- `2317192`: Damir, Samarqand shahar, Trikotajka
- `2350769`: Olim, Samarqand shahar, Pendjikentskaya
- `2704868`: Joni, Jombay tuman
- `2704868`: Joni, Jomboy
- `2732600`: Madina, Samarqand shahar, Ikar
- `3977793`: Umid, Payariq tuman, Chelak
- `4031181`: Dilshod, Paxtachi tuman, Ziyovuddin
- `5000104`: Ravshan, Samarqand shahar, Selskiy
- `5002637`: Sobir, Toyloq tuman
- `5054474`: Shoxrux, Samarqand shahar, Mikrorayon
- `5272978`: Shuxrat, Samarqand shahar, Gorgaz
- `5354034`: Olim, Samarqand shahar, Dagbitskiy
- `5438101`: Ixtier, Samarqand shahar, Kirpichka
- `5492676`: Nodir, Samarqand shahar, Trikotajka
- `5530021`: Zafar, Usto
- `5600101`: Amin, Samarqand shahar, Pavarot
- `5733979`: Akmal, Samarqand shahar, Lenin Bayroq
- `9214404`: Xusniddin, Payariq tuman, Chelak
- `9249464`: Furkat, Samarqand shahar
- `9258998`: Zafar, Samarqand shahar, Sogdiana
- `9276677`: Rajab, Samarqand shahar, Erkin Savdo
- `9975544`: Zafar, Urgut tuman

## ⚠️ Needs Your Review (23 entries)

These phone numbers have **different names** across sources. This could mean: two people sharing a phone, a business phone, or a data entry error. The cleanup kept the first/longer name, but please review:

### Phone: `901961001`
- **Mirzobek** — Payariq tuman, Nariman (source: Contacts)
- **Ulugbek** — Payariq tuman, Nariman (source: Contacts)
- **Currently kept:** Mirzobek

### Phone: `902833334`
- **Sobir** — Samarqand shahar, Kirpichka (source: Contacts)
- **Suxrob** — Samarqand shahar, Taksomotorniy (source: Contacts)
- **Currently kept:** Suxrob

### Phone: `906054040`
- **Bekzod** — Samarqand shahar, Afsona (source: Contacts)
- **Zoxid** — Samarqand shahar, Afsona (source: Contacts)
- **Currently kept:** Bekzod

### Phone: `913199932`
- **Baxriddin** — Nurobod tuman, Jom (source: Contacts)
- **Shapat** — Nurobod tuman (source: Contacts)
- **Currently kept:** Baxriddin

### Phone: `913373132`
- **Shavkat** — Xatirchi tuman (source: Contacts)
- **Shokir** — Samarqand shahar, Mirbozor (source: Contacts)
- **Currently kept:** Shavkat

### Phone: `915226660`
- **Kamol** — Urgut tuman (source: Contacts)
- **Shaxboz** — Urgut tuman (source: Contacts)
- **Currently kept:** Shaxboz

### Phone: `915272817`
- **Ilxom** — Urgut tuman (source: Contacts)
- **Inomjon** — Urgut tuman (source: Contacts)
- **Currently kept:** Inomjon

### Phone: `930060110`
- **Elyor** — Bulung'ur tuman (source: Contacts)
- **Eler** — Bulungur (source: Bulungur)
- **Currently kept:** Elyor

### Phone: `933338070`
- **Murod** — Samarqand shahar, Vokzal (source: Contacts)
- **Sardor** — Samarqand shahar, Pishchevoy (source: Contacts)
- **Currently kept:** Sardor

### Phone: `933492929`
- **Abror** — Toyloq tuman, Sochak (source: Contacts)
- **Sodik** — Toyloq tuman (source: Contacts)
- **Currently kept:** Sodik

### Phone: `933555605`
- **Xamrokul** — Toyloq tuman (source: Contacts)
- **Sherzod** — Toyloq tuman (source: Contacts)
- **Currently kept:** Xamrokul

### Phone: `941870505`
- **Firuz** — Jombay tuman (source: Contacts)
- **FEruz** — Jomboy (source: Jomboy)
- **Currently kept:** Firuz

### Phone: `942884144`
- **Jaxongir** — Payariq tuman, Narimon (source: Contacts)
- **Muyasar** — Payariq tuman (source: Contacts)
- **Currently kept:** Jaxongir

### Phone: `944756031`
- **Amir** — Payariq tuman, Chelak (source: Contacts)
- **Ergash** — Samarqand shahar, Metan (source: Contacts)
- **Currently kept:** Ergash

### Phone: `945370525`
- **Djafar** — Samarqand shahar, Elektroset (source: Contacts)
- **Jafar** — Samarqand shahar, Elektroset (source: Contacts)
- **Currently kept:** Djafar

### Phone: `946582494`
- **Zokir** — Payariq tuman, Nariman (source: Contacts)
- **Suxrob** — Payariq tuman, Nariman (source: Contacts)
- **Currently kept:** Suxrob

### Phone: `972957551`
- **Pardaboy** — G'allaorol tuman (source: Contacts)
- **Furkat** — G'allaorol tuman (source: Contacts)
- **Currently kept:** Pardaboy

### Phone: `979111991`
- **Dilshod** — Bulung'ur tuman (source: Contacts)
- **Toxir** — Bulung'ur tuman (source: Contacts)
- **Dilshod** — Bulungur (source: Bulungur)
- **Toxir** — Bulungur (source: Bulungur)
- **Currently kept:** Dilshod

### Phone: `979227844`
- **Mirzo** — Payariq tuman (source: Contacts)
- **Mirzobek** — Payariq tuman, Nariman (source: Contacts)
- **Currently kept:** Mirzobek

### Phone: `979228383`
- **Firuz** — Jombay tuman (source: Contacts)
- **FEruz** — Jomboy (source: Jomboy)
- **Currently kept:** Firuz

### Phone: `979247824`
- **Nuriddin** — Jombay tuman (source: Contacts)
- **Suxrob** — Jombay tuman (source: Contacts)
- **Nuriddin** — Jomboy (source: Jomboy)
- **Suxrob** — Jomboy (source: Jomboy)
- **Currently kept:** Nuriddin

### Phone: `982002328`
- **Avaz** — Samarqand shahar, Kirpichka (source: Contacts)
- **Jonibek** — Samarqand shahar, Kirpichka (source: Contacts)
- **Xamza** — Samarqand shahar, Dagbitskiy (source: Contacts)
- **Currently kept:** Jonibek

### Phone: `990719897`
- **Bobur** — Bulung'ur tuman, Mingchinor (source: Contacts)
- **Furkat** — Bulung'ur tuman, Mingchinor (source: Contacts)
- **Bobur** — Bulungur (source: Bulungur)
- **Furkat** — Bulungur (source: Bulungur)
- **Currently kept:** Furkat

