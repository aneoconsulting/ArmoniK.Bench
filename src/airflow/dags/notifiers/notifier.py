from typing import Sequence

from airflow.providers.amazon.aws.notifications.sns import SnsNotifier


class ArmoniKBenchEmailNotifier(SnsNotifier):
    template_fields: Sequence[str] = (*SnsNotifier.template_fields,)
    template_ext: Sequence[str] = ("html",)
