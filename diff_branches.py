from infrahub_sdk import Config, InfrahubClientSync

client = InfrahubClientSync(config=Config(address="http://localhost:8000"))
artifact_query = """
query ArtifactQuery {
  CoreArtifact {
    edges {
      node {
        checksum {
          value
        }
        storage_id {
          value
        }
      }
    }
  }
}
"""

data = client.execute_graphql(query=artifact_query, branch_name="main")
print("MAIN BRANCH")
for d in data["CoreArtifact"]["edges"]:
    print(f"checksum: {d['node']['checksum']['value']}, storage_id: {d['node']['storage_id']['value']}")
data = client.execute_graphql(query=artifact_query, branch_name="bla2")
print("BLA2 BRANCH")
for d in data["CoreArtifact"]["edges"]:
    print(f"checksum: {d['node']['checksum']['value']}, storage_id: {d['node']['storage_id']['value']}")
