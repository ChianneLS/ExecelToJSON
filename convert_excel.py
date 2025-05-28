import pandas as pd
import json

# === Load the Excel file ===
xls = pd.ExcelFile("Small Data Set.xlsx")

# === Load all necessary sheets ===
people_df = xls.parse("People")
address_df = xls.parse("Address")
lives_df = xls.parse("Lives at")
prefers_df = xls.parse("Prefers")
is_in_df = xls.parse("Is In")
boat_df = xls.parse("Boat")
race_df = xls.parse("Race")
has_df = xls.parse("Has")
participation_df = xls.parse("Participation")
seat_df = xls.parse("Seat")
coaches_df = xls.parse("Coaches")
coxswains_df = xls.parse("Coxs")

# === PEOPLE ===
def build_people_documents():
    merged = people_df.merge(lives_df, on="Mem_Id", how="left")
    merged = merged.merge(address_df, on=["Street Number", "Street Name", "City"], how="left")
    merged = merged.merge(prefers_df, on="Mem_Id", how="left")
    merged = merged.merge(is_in_df, on="Mem_Id", how="left")

    docs = []
    for _, row in merged.iterrows():
        doc = {"mem_id": int(row["Mem_Id"]),
               "fname": row["Fname"],
               "lname": row["Lname"],
               "gender": row["Gender"],
               "phone": str(row["Phone"]),
               "dob": str(row["DoB"]),
               "email": row["Email"]}
        if pd.notna(row.get("Preference_y")):
            doc["preference"] = row["Preference_y"]
        if pd.notna(row.get("Grade")):
            doc["grade"] = row["Grade"]
        if pd.notna(row.get("Street Number")):
            doc["address"] = {
                "street_number": row["Street Number"],
                "street_name": row["Street Name"],
                "city": row["City"],
                "postcode": row.get("Postcode")
            }
        docs.append(doc)
    return docs

# === BOATS ===
def build_boat_documents():
    docs = []
    for _, row in boat_df.iterrows():
        doc = {"boat_id": row["Boat_ID"],
               "name": row["Name"],
               "gender": row["Gender"],
               "weight_kg": row["Weight_kg"],
               "seats": int(row["Seats"]) }
        if pd.notna(row.get("Cox")):
            doc["cox"] = row["Cox"]
        docs.append(doc)
    return docs

# === REGATTAS ===
def build_regatta_documents():
    merged = has_df.merge(race_df, on="Title", how="left")
    regattas = []
    for (rname, year), group in merged.groupby(["RName", "Year"]):
        regatta = {"name": rname, "year": int(year)}
        races = []
        for _, row in group.iterrows():
            if pd.isna(row.get("Race #")): continue
            race = {"title": row["Title"],
                    "race_number": int(row["Race #"]) }
            # handle optional start time
            start = row.get("Start_Time") or row.get("Start Time")
            if pd.notna(start):
                race["start_time"] = str(start)
            length = row.get("Length (m)")
            if pd.notna(length):
                race["length_m"] = int(length)
            races.append(race)
        if races:
            regatta["races"] = races
        regattas.append(regatta)
    return regattas

# === PARTICIPATION ===
def build_participation_documents():
    docs = []
    for _, row in participation_df.iterrows():
        base = {}
        boat_id = row.get("Boat ID") or row.get("Boat_ID")
        if pd.notna(boat_id):
            base["boat_id"] = boat_id
        re = row.get("Race Event")
        if pd.notna(re):
            base["race_event"] = re
        title = row.get("Title")
        if pd.notna(title):
            base["title"] = title
        ln = row.get("Lane #")
        if pd.notna(ln):
            base["lane_number"] = ln
        rn = row.get("Race #")
        if pd.notna(rn):
            base["race_number"] = rn
        start = row.get("Start Time") or row.get("Start_Time")
        if pd.notna(start):
            base["start_time"] = str(start)
        outcome = row.get("Outcome")
        if pd.notna(outcome):
            base["outcome"] = outcome
        pen = row.get("Penalties")
        if pd.notna(pen):
            base["penalties"] = pen
        t = row.get("Time")
        if pd.notna(t):
            base["time"] = str(t)
        seats = seat_df[
            (seat_df.get("Race event") == re) &
            (seat_df.get("Title") == title) &
            (seat_df.get("Boat Id") == boat_id)
        ]
        seat_list = []
        for _, s in seats.iterrows():
            mem = s.get("Mem_no")
            snum = s.get("Seat Num")
            if pd.notna(mem) and pd.notna(snum):
                seat_list.append({"mem_id": int(mem), "seat_num": int(snum)})
        if seat_list:
            base["seats"] = seat_list
        docs.append(base)
    return docs

# === EXPORT SIMPLE COLLECTIONS ===
def export_simple_collection(df, filename):
    df.to_json(filename, orient="records", indent=2)

# === WRITE JSON FILES ===
with open("people_nested.json", "w") as f:
    json.dump(build_people_documents(), f, indent=2)
with open("boats.json", "w") as f:
    json.dump(build_boat_documents(), f, indent=2)
with open("regattas.json", "w") as f:
    json.dump(build_regatta_documents(), f, indent=2)
with open("participation.json", "w") as f:
    json.dump(build_participation_documents(), f, indent=2)
export_simple_collection(coaches_df, "coaches.json")
export_simple_collection(coxswains_df, "coxswains.json")
print("✅ All JSON files generated with nested and linked data ready for MongoDB import.")
