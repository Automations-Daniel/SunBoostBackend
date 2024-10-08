from notion_client import Client
import os

NOTION_TOKEN = os.getenv("NOTION_TOKEN")

# Inicializa el cliente de Notion
notion = Client(auth=NOTION_TOKEN)


def list_databases():
    """
    Función para listar todas las bases de datos disponibles en Notion.
    Devuelve una lista de tuplas con títulos y sus respectivos IDs.
    """
    databases_list = []
    try:
        databases = notion.search(filter={"property": "object", "value": "database"})
        for result in databases["results"]:
            title = result["title"][0]["plain_text"]
            databases_list.append((title, result["id"]))
            # print(f"ID: {result['id']}, Title: {title}")
    except Exception as e:
        print(f"Error buscando databases: {e}")
    return databases_list


def get_notion_data():
    """
    Función para obtener los campos ID y Link de todas las bases de datos de Notion.
    """
    data = []
    databases = list_databases()

    for title, database_id in databases:
        try:
            response = notion.databases.query(database_id=database_id)

            # Extraer los resultados
            for item in response.get("results", []):
                row = {"ID": None, "Link": None}

                # Verificar y obtener el ID
                try:
                    if item["properties"]["ID"]["rich_text"]:
                        row["ID"] = item["properties"]["ID"]["rich_text"][0]["text"][
                            "content"
                        ]
                except (IndexError, KeyError):
                    print(f"ID no disponible para el elemento: {item}")

                # Verificar y obtener el Link
                try:
                    if (
                        "Link" in item["properties"]
                        and item["properties"]["Link"]["url"]
                    ):
                        row["Link"] = item["properties"]["Link"]["url"]
                except KeyError:
                    print(f"Link no disponible para el elemento: {item}")

                data.append(row)

        except Exception as e:
            print(f"Error consultando Notion para la base de datos {title}: {e}")

    return data


if __name__ == "__main__":
    notion_data = get_notion_data()
    print(notion_data)
