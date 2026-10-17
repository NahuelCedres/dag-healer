"""FastAPI sub-application mounted by the Airflow plugin under ``/healer``.

See :mod:`dag_healer.api.app`. Nothing is imported here so that importing
``dag_healer.api.<module>`` stays cheap.
"""
