class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, price: float) -> None:
        self.count += 1
        self.total += price

    @property
    def average(self) -> float:
        if self.count == 0:
            return 0.0
        return self.total / self.count
