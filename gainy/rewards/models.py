from gainy.data_access.models import BaseModel, classproperty


class InvitationCashReward(BaseModel):
    invitation_id = None
    profile_id = None
    money_flow_id = None
    created_at = None

    key_fields = ["invitation_id", "profile_id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["created_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "invitation_cash_rewards"
