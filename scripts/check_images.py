"""Check pinned public image manifests, without a Docker installation."""

import json
from urllib.request import Request, urlopen

IMAGES = {
    "apache/spark": "3.5.6-scala2.12-java17-python3-ubuntu",
    "apache/kafka": "3.9.0",
    "library/postgres": "16.6-bookworm",
    "library/python": "3.11.11-slim-bookworm",
    "library/eclipse-temurin": "11-jre-jammy",
}


def main():
    for repository, tag in IMAGES.items():
        with urlopen(
            "https://auth.docker.io/token?service=registry.docker.io&scope=repository:"
            + repository
            + ":pull",
            timeout=30,
        ) as response:
            token = json.load(response)["token"]
        request = Request(
            f"https://registry-1.docker.io/v2/{repository}/manifests/{tag}",
            method="HEAD",
            headers={
                "Authorization": "Bearer " + token,
                "Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json",
            },
        )
        with urlopen(request, timeout=30) as response:
            print(repository + ":" + tag, response.status, response.headers.get("Docker-Content-Digest"))


if __name__ == "__main__":
    main()
