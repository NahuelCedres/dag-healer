"""Remote dataset storage readers (imported only when a locator names a remote scheme).

v1 has S3 (:mod:`dag_healer.storage.s3`, the ``aws`` extra): ``datasets.load_dataset``
dispatches ``s3://bucket/key`` locators to it before its local-path logic.
"""
