# Depot Release Process

Releases are currently manually performed.

1) Upgrade version in `Cargo.toml`
2) Commit changes
3) Create and push a tag: ```git tag v<version>; git push origin v<version>```
4) Release on crates.io: ```cargo publish```
