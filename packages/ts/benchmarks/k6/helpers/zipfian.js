export class ZipfianGenerator {
  constructor(items, theta = 0.99) {
    this.items = items
    this.theta = theta
    this.zetaN = this._computeZeta(items, theta)
    const zeta2 = this._computeZeta(2, theta)
    this.alpha = 1.0 / (1.0 - theta)
    this.eta = (1.0 - (2.0 / items) ** (1.0 - theta)) / (1.0 - zeta2 / this.zetaN)
  }

  next() {
    const u = Math.random()
    const uz = u * this.zetaN
    if (uz < 1.0) return 0
    if (uz < 1.0 + 0.5 ** this.theta) return 1
    return Math.floor(this.items * (this.eta * u - this.eta + 1.0) ** this.alpha)
  }

  _computeZeta(n, theta) {
    let sum = 0
    for (let i = 0; i < n; i++) {
      sum += 1.0 / (i + 1) ** theta
    }
    return sum
  }
}
