from bubuku.controller import Change


class BaseRebalanceChange(Change):
    def get_name(self) -> str:
        return 'rebalance'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'restart', 'rebalance', 'stop']])

    @staticmethod
    def should_be_paused(current_actions):
        return any([a in current_actions for a in ['restart', 'start', 'stop']])