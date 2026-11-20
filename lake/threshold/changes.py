from dataclasses import dataclass

from lake.reports.rollup import LiveTotals


@dataclass(frozen=True, slots=True)
class ThresholdChange:
    request_id: str
    fact_cut: int
    position: int
    before: LiveTotals
    after: LiveTotals


@dataclass(frozen=True, slots=True)
class FeedThresholdChange:
    """A threshold change delivered through read_feed at a reporting position."""

    change: ThresholdChange

    @property
    def position(self):
        return self.change.position

    @property
    def request_id(self):
        return self.change.request_id

    @property
    def fact_cut(self):
        return self.change.fact_cut

    @property
    def before(self):
        return self.change.before

    @property
    def after(self):
        return self.change.after
