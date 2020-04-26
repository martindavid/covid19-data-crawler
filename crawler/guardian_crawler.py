import requests
import datetime
from models.base import Session
from models import GuardianAustraliaData, AustraliaLatest


class GuardianDataCrawler():
    def __init__(self):
        self.session = Session()

    def crawl_data(self):
        print("[START] Crawl Guardian Australian data")
        url = "https://interactive.guim.co.uk/docsdata/1q5gdePANXci8enuiS4oHUJxcxC13d6bjMRSicakychE.json"
        response = requests.get(url)
        data = response.json()

        self.crawl_daily_data(data)
        self.crawl_latest_data(data)
        print("[END] Crawl Guardian Australian data")

    def crawl_latest_data(self, data):
        self.session.query(AustraliaLatest).delete(synchronize_session=False)
        idx = 1
        print("[START] Crawl latest Australia stats")
        for row in data["sheets"]["latest totals"]:
           self.session.add(AustraliaLatest(
               state=row["State or territory"],
               state_name=row["Long name"],
               confirmed=parseEmptyStringToInteger(row["Confirmed cases (cumulative)"]),
               deaths=parseEmptyStringToInteger(row["Deaths"]),
               recovered=parseEmptyStringToInteger(row["Recovered"]),
               active_cases=parseEmptyStringToInteger(row["Active cases"]),
               test_conducted=parseEmptyStringToInteger(row["Tests conducted"]),
               tests_per_million=parseEmptyStringToInteger(row["Tests per million"]),
               percent_positive=row["Percent positive"],
               current_hospitalisation=row["Current hospitalisation"],
               current_icu=row["Current ICU"],
               current_in_ventilator=parseEmptyStringToInteger(row["Current ventilator use"]),
               last_updated=row["Last updated"]
           ))
        self.session.commit()
        print("[END] Crawl latest Australia stats")

    def crawl_daily_data(self, data):
        print("[START] Crawl daily updates data")
        self.session.query(GuardianAustraliaData).delete(synchronize_session=False)
        idx = 1

        for row in data["sheets"]["updates"]:
            self.session.add(GuardianAustraliaData(
                community=parseEmptyStringToInteger(row.get("Community", 0)),
                community_unknown=parseEmptyStringToInteger(row.get("Community - no known source", 0)),
                confirmed=parseEmptyStringToInteger(row.get("Cumulative case count", 0)),
                recovered=parseEmptyStringToInteger(row.get("Recovered (cumulative)", 0)),
                deaths=parseEmptyStringToInteger(row.get("Cumulative deaths", 0)),
                date=parseDateString(row.get("Date")),
                hospitalisation=parseEmptyStringToInteger(row.get("Hospitalisations (count)", 0)),
                intensive_care=parseEmptyStringToInteger(row.get("Intensive care (count)", 0)),
                notes=row.get("Notes", ""),
                under_60=parseEmptyStringToInteger(row.get("Under 60", 0)),
                over_60=parseEmptyStringToInteger(row.get("Over 60", 0)),
                state=row.get("State", ""),
                test_conducted_neg=parseEmptyStringToInteger(row.get("Tests conducted (negative)", 0)),
                test_conducted_tot=parseEmptyStringToInteger(row.get("Tests conducted (total)", 0)),
                travel_related=parseEmptyStringToInteger(row.get("Travel-related", 0)),
                under_investigation=parseEmptyStringToInteger(row.get("Under investigation", 0)),
                update_source=row.get("Update Source", 0),
                ventilator_usage=parseEmptyStringToInteger(row.get("Ventilator usage (count)", 0))
            ))
            idx += 1

        self.session.commit()
        print(f"Successfully insert {idx} rows of Guardian australian data")
        print("[END] Crawl daily updates data")


def parseDateString(data):
    parsed_date = None if data == "" else datetime.datetime.strptime(data, '%d/%m/%Y').strftime('%Y-%m-%d')
    return parsed_date


def parseEmptyStringToInteger(data):
    updated_string = data.strip().replace(",", "")
    return 0 if updated_string == "" or updated_string == "-" else int(updated_string)
