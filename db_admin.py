import os

from app_utils import ENV_ID, USER_ID
from fb_core.real_time_database import FirebaseRTDBManager


class DBAdmin:

    def __init__(self, user_id, env_id):
        self.user_id = user_id
        self.env_id = env_id
        self.database = f"users/{self.user_id}/env/{self.env_id}"
        self.metadata_path = "metadata"
        self.states_path = "global_states"

        self.db_manager = FirebaseRTDBManager(
            database_url=os.environ.get("FIREBASE_RTDB"),
            base_path=self.database,
        )


    def change_state(self, state=None):
        """Changes state of ALL metadata entries"""
        upsert_data = {}
        data = self.db_manager.get_data(path=self.metadata_path)
        ready = None
        for mid, data in data["metadata"].items():
            if state is None:
                current_state = data["status"]["state"]
                if current_state == "active":
                    new_state = "inactive"
                    if ready is None:
                        ready=False
                else:
                    new_state = "active"
                    if ready is None:
                        ready = True
            else:
                new_state = state
                ready = False

            upsert_data[f"{mid}/status/state/"] = new_state

        self.db_manager.update_data(
            path=self.metadata_path,
            data=upsert_data
        )

        if state is not None:
            if state == "INACTIVE":
                ready = False
            elif state == "ACTIVE":
                ready = True

        print("Upsert glbal states")
        global_data={"ready": ready}
        global_path = f"{self.database}/{self.states_path}/"
        self.db_manager.update_data(
            path=global_path,
            data=global_data
        )


if __name__ == "__main__":
    admin = DBAdmin(env_id=ENV_ID, user_id=USER_ID)
    admin.change_state(state="INACTIVE")
