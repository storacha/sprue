# Sprue

The Storacha upload service in Go.

## Notes

* Rate limits storage was not implemented. It has never been used in JS implementation, only supports blocking completely and can probably be applied at firewall.
* Plans, provisions, subscriptions, usage are not stores, they are services.
* The following dynamo tables have GSIs that do not exist in w3infra that need to be added:
    * `consumer` - `consumerV3` and `customerV2`
