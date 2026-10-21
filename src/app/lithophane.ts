// Port of lithophane.py (single color + RGB-swap mode) with a curved panel, base and hole.
export type Mode = 'single' | 'rgb';
export interface Params {
  width: number; arc: number; hole: number; baseH: number; border: number;
  minT: number; maxT: number; pixel: number; gamma: number; invert: boolean;
  mode: Mode; stripe: number; layerH: number; baseShape: 'semi' | 'full';
}
export const DEFAULTS: Params = {
  width: 130, arc: 180, hole: 30, baseH: 3, border: 2, minT: 0.8, maxT: 3,
  pixel: 0.2, gamma: 1, invert: false, mode: 'single', stripe: 2, layerH: 0.2, baseShape: 'semi',
};
export interface Stripe { c: number; z: number; fl: number }
export const NAMES = ['Red', 'Green', 'Blue'];
const TINT = [[1, .12, .12], [.12, 1, .12], [.15, .25, 1]];
const RING_EXTRA = 5;
const COLLAR = 6;   // ring of base kept around the hole in semicircle mode

function stripePlan(h: number, stripe: number, lh: number, fh: number): Stripe[] {
  const s: Stripe[] = [{ c: 0, z: 0, fl: 1 }];
  let prev = -1, m = 1;
  for (;;) {
    const j = Math.max(Math.round((m * stripe - fh) / lh), prev + 1);
    const z = fh + j * lh;
    if (z > h - lh * 0.5) break;
    s.push({ c: m % 3, z, fl: j + 2 });
    prev = j; m++;
  }
  return s;
}

export interface Prep { rows: number; cols: number; th: Float32Array; col: Float32Array; stripes: Stripe[]; height: number }

let cache: { img: HTMLImageElement; c: number; r: number; data: Uint8ClampedArray } | null = null;

export function prepare(img: HTMLImageElement, p: Params): Prep {
  if (p.mode === 'rgb' && p.stripe < p.layerH) throw new Error('Stripe height cannot be below the layer height.');
  const n = p.border > 0 ? Math.max(1, Math.round(p.border / p.pixel)) : 0;
  const c0 = Math.max(2, Math.round(p.width / p.pixel) + 1);
  const r0 = Math.max(2, Math.round((c0 - 1) * img.naturalHeight / img.naturalWidth) + 1);
  let d: Uint8ClampedArray;
  if (cache && cache.img === img && cache.c === c0 && cache.r === r0) d = cache.data;
  else {
    const cv = document.createElement('canvas'); cv.width = c0; cv.height = r0;
    const cx = cv.getContext('2d', { willReadFrequently: true })!;
    cx.fillStyle = '#fff'; cx.fillRect(0, 0, c0, r0); cx.drawImage(img, 0, 0, c0, r0);
    d = cx.getImageData(0, 0, c0, r0).data;
    cache = { img, c: c0, r: r0, data: d };
  }
  const rows = r0 + 2 * n, cols = c0 + 2 * n;
  const off = p.baseH - Math.min(1, p.baseH / 2);
  const height = off + (rows - 1) * p.pixel;

  let stripes: Stripe[] = [];
  const rc = new Uint8Array(rows);
  if (p.mode === 'rgb') {
    stripes = stripePlan(height, p.stripe, p.layerH, p.layerH);
    const starts = stripes.slice(1).map(s => s.z);
    for (let r = 0; r < rows; r++) {
      const z = off + (rows - 1 - r) * p.pixel + 1e-6;
      let k = 0; while (k < starts.length && starts[k] <= z) k++;
      rc[r] = k % 3;
    }
  }
  const th = new Float32Array(rows * cols).fill(p.maxT);
  const col = new Float32Array(rows * cols * 3).fill(0.03);
  for (let r = 0; r < r0; r++) for (let c = 0; c < c0; c++) {
    const i = (r + n) * cols + c + n, q = (r * c0 + c) * 4;
    const v = [0, 1, 2].map(k => {
      let x = d[q + k] / 255; if (p.invert) x = 1 - x;
      return p.gamma !== 1 ? Math.pow(x, p.gamma) : x;
    });
    const ch = rc[r + n];
    const lum = p.mode === 'rgb' ? v[ch] : 0.299 * v[0] + 0.587 * v[1] + 0.114 * v[2];
    th[i] = p.minT + (p.maxT - p.minT) * (1 - lum);
    const t = p.mode === 'rgb' ? TINT[ch] : [1, .93, .78];
    col[i * 3] = lum * t[0]; col[i * 3 + 1] = lum * t[1]; col[i * 3 + 2] = lum * t[2];
  }
  return { rows, cols, th, col, stripes, height };
}

export interface Mesh { V: Float32Array; F: Uint32Array; C: Float32Array; outerR: number; radius: number; height: number }

interface Ring { rIn: number; rOut: number; a0: number; span: number; full: boolean; m: number; pts: number }

export function buildMesh(p: Params, d: Prep): Mesh {
  const { rows, cols, th, col } = d;
  const n = rows * cols, px = p.pixel;
  const width = (cols - 1) * px, theta = p.arc * Math.PI / 180, R = width / theta, hr = p.hole / 2;
  if (hr > R - 2) throw new Error(`Hole too big: the inside diameter is only ${(2 * R).toFixed(0)} mm. Widen the picture, lower the arc or shrink the hole.`);
  const z0 = p.baseH - Math.min(1, p.baseH / 2);
  let tmax = 0; for (let i = 0; i < n; i++) if (th[i] > tmax) tmax = th[i];
  const Ro = R + tmax + RING_EXTRA;

  // Base = one or two ring shells. The picture sits at angle -90 deg (the -y side).
  const mk = (rIn: number, rOut: number, a0: number, span: number, full: boolean): Ring => {
    const m = Math.max(24, Math.round(span * rOut / 0.4));
    return { rIn, rOut, a0, span, full, m, pts: full ? m : m + 1 };
  };
  const S = Math.max(Math.PI, theta);   // semicircle, widened if the picture arc is bigger
  const rings: Ring[] = p.baseShape === 'semi'
    ? [mk(hr, Ro, -Math.PI / 2 - S / 2, S, false), mk(hr, Math.min(Ro, hr + COLLAR), 0, 2 * Math.PI, true)]
    : [mk(hr, Ro, 0, 2 * Math.PI, true)];

  const V = new Float32Array((2 * n + rings.reduce((a, g) => a + 4 * g.pts, 0)) * 3), C = new Float32Array(V.length);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) {
    const i = r * cols + c, a = (c * px / width - 0.5) * theta, s = Math.sin(a), k = Math.cos(a);
    const y = (rows - 1 - r) * px + z0, rf = R + th[i];
    V.set([rf * s, -rf * k, y], i * 3);
    V.set([R * s, -R * k, y], (n + i) * 3);
    C.set([col[i * 3], col[i * 3 + 1], col[i * 3 + 2]], i * 3);
    C.fill(0.12, (n + i) * 3, (n + i) * 3 + 3);
  }
  let vo = 2 * n;
  const bases = rings.map(g => {
    const OT = vo, OB = OT + g.pts, IT = OB + g.pts, IB = IT + g.pts; vo = IB + g.pts;
    for (let j = 0; j < g.pts; j++) {
      const t = g.a0 + g.span * j / g.m, c = Math.cos(t), s = Math.sin(t);
      V.set([g.rOut * c, g.rOut * s, p.baseH], (OT + j) * 3); V.set([g.rOut * c, g.rOut * s, 0], (OB + j) * 3);
      V.set([g.rIn * c, g.rIn * s, p.baseH], (IT + j) * 3); V.set([g.rIn * c, g.rIn * s, 0], (IB + j) * 3);
    }
    return { OT, OB, IT, IB };
  });
  C.fill(0.25, 2 * n * 3);

  const loop: number[] = [];
  for (let c = 0; c < cols; c++) loop.push((rows - 1) * cols + c);
  for (let r = rows - 2; r >= 0; r--) loop.push(r * cols + cols - 1);
  for (let c = cols - 2; c >= 0; c--) loop.push(c);
  for (let r = 1; r < rows - 1; r++) loop.push(r * cols);

  const ringFaces = rings.reduce((a, g) => a + 8 * g.m + (g.full ? 0 : 4), 0);
  const F = new Uint32Array((4 * (rows - 1) * (cols - 1) + 2 * loop.length + ringFaces) * 3);
  let k = 0;
  const tri = (a: number, b: number, c: number) => { F[k++] = a; F[k++] = b; F[k++] = c; };
  for (let r = 0; r < rows - 1; r++) for (let c = 0; c < cols - 1; c++) {
    const a = r * cols + c, b = a + 1, e = a + cols, f = e + 1;
    tri(a, e, f); tri(a, f, b); tri(n + a, n + f, n + e); tri(n + a, n + b, n + f);
  }
  for (let m = 0; m < loop.length; m++) {
    const a = loop[m], b = loop[(m + 1) % loop.length];
    tri(a + n, b + n, b); tri(a + n, b, a);
  }
  rings.forEach((g, ri) => {
    const { OT, OB, IT, IB } = bases[ri];
    for (let j = 0; j < g.m; j++) {
      const m = g.full ? (j + 1) % g.m : j + 1;
      tri(OT + j, OT + m, IT + m); tri(OT + j, IT + m, IT + j);
      tri(OB + j, IB + m, OB + m); tri(OB + j, IB + j, IB + m);
      tri(OB + j, OB + m, OT + m); tri(OB + j, OT + m, OT + j);
      tri(IB + j, IT + m, IB + m); tri(IB + j, IT + j, IT + m);
    }
    if (!g.full) {                       // flat end faces of the half ring
      const e = g.m;
      tri(IB, OB, OT); tri(IB, OT, IT);
      tri(IB + e, OT + e, OB + e); tri(IB + e, IT + e, OT + e);
    }
  });
  return { V, F, C, outerR: Ro, radius: R, height: d.height };
}

export function toStl(m: Mesh): Blob {
  const nt = m.F.length / 3, buf = new ArrayBuffer(84 + 50 * nt), dv = new DataView(buf);
  dv.setUint32(80, nt, true);
  const P = (i: number) => [m.V[i * 3] + m.outerR, m.V[i * 3 + 1] + m.outerR, m.V[i * 3 + 2]];
  for (let t = 0; t < nt; t++) {
    const a = P(m.F[t * 3]), b = P(m.F[t * 3 + 1]), c = P(m.F[t * 3 + 2]);
    const u = [b[0] - a[0], b[1] - a[1], b[2] - a[2]], w = [c[0] - a[0], c[1] - a[1], c[2] - a[2]];
    const nx = u[1] * w[2] - u[2] * w[1], ny = u[2] * w[0] - u[0] * w[2], nz = u[0] * w[1] - u[1] * w[0];
    const l = Math.hypot(nx, ny, nz) || 1, o = 84 + t * 50;
    [nx / l, ny / l, nz / l, ...a, ...b, ...c].forEach((v, i) => dv.setFloat32(o + i * 4, v, true));
  }
  return new Blob([buf], { type: 'model/stl' });
}

export function swapSheet(p: Params, d: Prep): string {
  const s = d.stripes, L = ['LITHOPHANE COLOR SWAP SHEET', '='.repeat(50),
    `Layer height ${p.layerH} mm, first layer ${p.layerH} mm, 100% infill`,
    'Print standing exactly as exported (base on the bed).',
    'Start with RED. Pause BEFORE the layer listed, swap, purge, resume.', '',
    'Swap  Pause before layer  Layer Z   Load'];
  s.slice(1).forEach((x, i) => L.push(`${String(i + 1).padStart(4)}  ${String(x.fl).padStart(18)}  ${(p.layerH + (x.fl - 1) * p.layerH).toFixed(2).padStart(7)}   ${NAMES[x.c].toUpperCase()}`));
  return L.join('\n') + '\n';
}