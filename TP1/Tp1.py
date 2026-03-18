import requests
import csv
import re
import time

topics = [
    "Internet of Things","Industrial Internet of Things","Smart city",
    "Wireless sensor network","Edge computing","Cloud computing",
    "MQTT","Embedded system","Home automation","Cyber-physical system",
    "Internet of things security","Wearable technology","Machine to machine",
    "Big data","Artificial intelligence","Radio-frequency identification",
    "Smart grid","Software-defined networking","Sensor","Actuator"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def clean_text(text):
    text = re.sub(r'\[\d+\]', '', text)
    text = re.sub(r'==.*?==', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_category(text):
    t = text.lower()
    if any(x in t for x in ['sensor','device','hardware','chip']):
        return "Hardware"
    if any(x in t for x in ['protocol','network','wifi','mqtt','communication']):
        return "Connectivity"
    if any(x in t for x in ['security','attack','privacy','encrypt']):
        return "Security"
    if any(x in t for x in ['cloud','data','computing','analytics']):
        return "Infrastructure/AI"
    return "General IoT"

all_data = []
seen = set()

print("🚀 Starting dataset collection...")

for topic in topics:

    if len(all_data) >= 1300:
        break

    url = "https://en.wikipedia.org/w/api.php"

    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "explaintext": True,
        "titles": topic
    }

    success = False

    for attempt in range(3):   # إعادة المحاولة 3 مرات
        try:

            response = requests.get(
                url,
                params=params,
                headers=HEADERS,
                timeout=25
            )

            data = response.json()
            success = True
            break

        except Exception as e:
            print(f"⚠ Retry {attempt+1} for {topic}")
            time.sleep(3)

    if not success:
        print(f"❌ Skipped: {topic}")
        continue

    pages = data.get("query", {}).get("pages", {})

    for page in pages.values():

        content = page.get("extract", "")
        if not content:
            continue

        text = clean_text(content)
        sentences = re.split(r'(?<=[.!?])\s+', text)

        for s in sentences:

            s = s.strip()

            if len(s) > 65 and s[0].isupper() and s not in seen:

                seen.add(s)

                all_data.append({
                    "Source_Topic": topic,
                    "Information_Text": s,
                    "Category": get_category(s),
                    "Char_Count": len(s)
                })

    print(f"✅ {topic} collected | total: {len(all_data)}")

    time.sleep(1)   # إبطاء الطلبات حتى لا تمنعنا ويكيبيديا

final_data = all_data[:1200]

with open("../TP2/iot_final_dataset.csv", "w", newline="", encoding="utf-8-sig") as f:

    writer = csv.DictWriter(
        f,
        fieldnames=[
            "Source_Topic",
            "Information_Text",
            "Category",
            "Char_Count"
        ]
    )

    writer.writeheader()
    writer.writerows(final_data)

print("--------------------------------------------------")
print("Dataset size:",len(final_data))
print("Saved file: iot_final_dataset.csv")
print("--------------------------------------------------")