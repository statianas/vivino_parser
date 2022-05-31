
import requests
import pandas as pd
import urllib





country_code = {"ar": "Argentina", "at": "Austria", "au":"Australia", "br":"Brazil", "cl": "Chile", "de":"Germany", "es":"Spain", "fr":"France", "gb":"United Kingdom", "ge":"Georgia", "gr":"Greece", "it":"Italy", "pt":"Portugal", "us": "USA" }
type_code = {"1":"Red", "2":"White", "3":"Sparkling", "4": "Rose", "7": "Dessert", "24":"Fortified"}
def get_wine_data(wine_id, year, page):
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    }

    api_url = "https://www.vivino.com/api/wines/{id}/reviews?per_page=50&year={year}&page={page}"

    data = requests.get(
        api_url.format(id=wine_id, year=year, page=page), headers=headers
    ).json()


    return data


for i in range(1, 100):
    try:
        r = requests.get(
            "https://www.vivino.com/api/explore/explore",
            params={
                "country_code": "IT",
                "country_codes[]": ["ar", "at", "au", "br", "cl", "de", "es", "fr", "gb", "ge", "gr", "it", "pt", "us"],
                "currency_code": "EUR",
                # "grape_filter": "varietal",
                "min_rating": "1",
                "order_by": "price",
                "order": "asc",
                "page": i,
                "price_range_max": "10000",
                "price_range_min": "0",

                "region_ids[]": 394,
            },
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
            },
        )

        if not r.json()["explore_vintage"]:
            break

        results = [
            (
                t["vintage"]["wine"]["winery"]["name"],
                t["vintage"]["year"],
                t["vintage"]["wine"]["id"],
                f'{t["vintage"]["wine"]["name"]} {t["vintage"]["year"]}',
                t["vintage"]["statistics"]["ratings_average"],
                t["vintage"]["statistics"]["ratings_count"],
                type_code[str(t["vintage"]["wine"]["type_id"])],
                f'{t["vintage"]["wine"]["region"]["country"]["name"]} {t["vintage"]["wine"]["region"]["name"]}',
                t["vintage"]["wine"]["region"]["country"]["code"],
                f'{t["vintage"]["wine"]["region"]["country"]["most_used_grapes"][0]["name"]} {t["vintage"]["wine"]["region"]["country"]["most_used_grapes"][1]["name"]}',
                round(t["price"]["amount"], 2) if t["price"] is not None else "-",
                f'{"https://www.vivino.com/IT/en/"}{t["vintage"]["seo_name"]}{"/w/"}{t["vintage"]["wine"]["id"]}'
            )
            for t in r.json()["explore_vintage"]["matches"]
        ]
        dataframe = pd.DataFrame(
            results,
            columns=["Winery", "Year", "Wine ID", "Wine", "Rating", "num_review", "Wine type", "Wine region", "Country", "Grape", "price", "link"],
        )
        ratings = []
        for _, row in dataframe.iterrows():
            page = 1
            while True:
                print(
                    f'Getting info about wine {row["Wine ID"]}-{row["Year"]} Page {page}'
                )

                d = get_wine_data(row["Wine ID"], row["Year"], page)

                if not d["reviews"]:
                    break
                if not d["reviews"][0]["user"]["statistics"]:

                    break

                for r in d["reviews"]:
                    if not r["user"]["statistics"]:
                        break
                    ratings.append(
                        [
                            row["Year"],
                            row["Wine ID"],
                            r["rating"],
                            r["note"],
                            r["created_at"],
                            r["user"]["id"],
                            r["user"]["statistics"]["followers_count"],
                            r["user"]["statistics"]["followings_count"],
                            r["user"]["statistics"]["ratings_count"],
                            r["language"]
                        ]
                    )

                page += 1





        ratings = pd.DataFrame(
            ratings, columns=["Year", "Wine ID", "User's Grade", "Note", "CreatedAt", "user id", "followers", "following", "User Ratings", "language"]
        )

        df_out = dataframe.merge(ratings)
        string = "wines_" + str(i) + ".csv"
        df_out.to_csv(string, index=False, sep = ';')
        print("Completed ",i)
    except Exception:
        print(f"Exception occured on {i}")
        continue