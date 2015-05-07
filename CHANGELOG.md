## 0.3.7

* [#10][]: Checking for empty batch before submitting
* [#12][]: added the ability to specify the max batch size for ES bulk operations

## 0.3.6

* Fixing issue with makeState() instantiating multiple Node|Transport clients (one for each call).

Now, clients are re-used across makeState() calls. A map is used to allow for multiple
connections across clusters. The key for the map is the clusterName|clusterHosts. The value is
the node|client.

## 0.3.4

* [#8][]: Allowing the index mapper to pass back a null document or id to avoid indexing

## 0.3.3

* Add batching support

## 0.3.2

* No changes

## 0.3.1

* Upgraded to ES 1.0.0

## 0.3.0

* Upgraded to ES 1.0.0-RC2

## 0.1.0

* Initial release with ES 0.90.5

[#8]: https://github.com/hmsonline/trident-elasticsearch/pulls/8
[#10]: https://github.com/hmsonline/trident-elasticsearch/pulls/10
[#12]: https://github.com/hmsonline/trident-elasticsearch/pulls/12
