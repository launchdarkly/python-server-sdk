from urllib.parse import urlparse, urlunparse

# The key, after the prefix, that marks the store as initialized. Other LaunchDarkly SDKs use the
# same key, so they can share one Redis database.
INITED_KEY = '$inited'


def redact_password(url: str) -> str:
    """
    Replace any embedded password in the provided URL with 'xxxx'. This is
    useful for ensuring sensitive information included in a URL isn't logged.
    """
    parts = urlparse(url)
    if parts.password is None:
        return url

    updated = parts.netloc.replace(parts.password, "xxxx")
    parts = parts._replace(netloc=updated)

    return urlunparse(parts)
