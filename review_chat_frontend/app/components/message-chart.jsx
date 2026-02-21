'use client';

import { useMemo } from 'react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  LabelList,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

const PALETTE = ['#38bdf8', '#f59e0b', '#34d399', '#fb7185', '#a78bfa', '#f97316'];

function toNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatValue(value, format) {
  const numeric = toNumber(value);
  if (numeric === null) return String(value ?? '');

  if (format === 'currency') {
    return `${Math.round(numeric).toLocaleString()}원`;
  }
  if (format === 'count') {
    return `${Math.round(numeric).toLocaleString()}건`;
  }
  if (format === 'percent') {
    return `${numeric.toFixed(1)}%`;
  }
  return numeric.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function formatTick(value) {
  const numeric = toNumber(value);
  if (numeric === null) return String(value ?? '');
  if (Math.abs(numeric) >= 100000000) return `${(numeric / 100000000).toFixed(1)}억`;
  if (Math.abs(numeric) >= 10000) return `${(numeric / 10000).toFixed(1)}만`;
  return Math.round(numeric).toLocaleString();
}

function formatTickByFormat(value, format) {
  const numeric = toNumber(value);
  if (numeric === null) return String(value ?? '');
  if (format === 'percent') return `${numeric.toFixed(1)}%`;
  if (format === 'currency') {
    if (Math.abs(numeric) >= 100000000) return `${(numeric / 100000000).toFixed(1)}억`;
    if (Math.abs(numeric) >= 10000) return `${(numeric / 10000).toFixed(1)}만`;
    return Math.round(numeric).toLocaleString();
  }
  return formatTick(numeric);
}

function formatAssistLabel(value, format) {
  const numeric = toNumber(value);
  if (numeric === null) return '';
  if (format === 'percent') return `${numeric.toFixed(1)}%`;
  if (format === 'currency') return `${Math.round(numeric).toLocaleString()}원`;
  return Math.round(numeric).toLocaleString();
}

function parseChartSpec(raw) {
  if (!raw || typeof raw !== 'string') return null;

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!parsed || typeof parsed !== 'object') return null;

  const chartType = ['line', 'bar', 'area'].includes(parsed.chartType) ? parsed.chartType : 'line';
  const xKey = typeof parsed.xKey === 'string' && parsed.xKey.trim() ? parsed.xKey : 'x';
  const data = Array.isArray(parsed.data) ? parsed.data : [];
  if (data.length === 0) return null;

  const candidateSeries = Array.isArray(parsed.series) ? parsed.series : [];
  const normalizedSeries = candidateSeries
    .filter((series) => series && typeof series.key === 'string')
    .map((series, idx) => ({
      key: series.key,
      label: typeof series.label === 'string' ? series.label : series.key,
      format: ['currency', 'count', 'percent', 'number'].includes(series.format)
        ? series.format
        : 'number',
      color: typeof series.color === 'string' ? series.color : PALETTE[idx % PALETTE.length],
    }));

  const derivedSeries =
    normalizedSeries.length > 0
      ? normalizedSeries
      : Object.keys(data[0] || {})
          .filter((key) => key !== xKey)
          .map((key, idx) => ({
            key,
            label: key,
            format: 'number',
            color: PALETTE[idx % PALETTE.length],
          }));

  if (derivedSeries.length === 0) return null;

  const normalizedData = data
    .map((row) => {
      if (!row || typeof row !== 'object') return null;
      const next = { [xKey]: String(row[xKey] ?? '') };
      for (const series of derivedSeries) {
        const numeric = toNumber(row[series.key]);
        next[series.key] = numeric ?? 0;
      }
      return next;
    })
    .filter(Boolean);

  if (normalizedData.length === 0) return null;

  return {
    title: typeof parsed.title === 'string' ? parsed.title : '그래프',
    subtitle: typeof parsed.subtitle === 'string' ? parsed.subtitle : '',
    chartType,
    xKey,
    data: normalizedData,
    series: derivedSeries,
  };
}

function CustomTooltip({ active, payload, label, seriesByKey }) {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-label">{label}</div>
      {payload.map((entry) => {
        const key = String(entry.dataKey);
        const meta = seriesByKey[key];
        return (
          <div className="chart-tooltip-row" key={key}>
            <span className="chart-tooltip-dot" style={{ backgroundColor: entry.color }} />
            <span>{meta?.label || key}</span>
            <strong>{formatValue(entry.value, meta?.format)}</strong>
          </div>
        );
      })}
    </div>
  );
}

function buildAxisPlan(spec) {
  const stats = spec.series.map((series) => {
    const maxAbs = spec.data.reduce((maxValue, row) => {
      const numeric = toNumber(row[series.key]);
      if (numeric === null) return maxValue;
      return Math.max(maxValue, Math.abs(numeric));
    }, 0);
    return { key: series.key, format: series.format, maxAbs };
  });

  const meaningful = stats.filter((item) => item.maxAbs > 0);
  const maxAbs = meaningful.length ? Math.max(...meaningful.map((item) => item.maxAbs)) : 0;
  const minAbs = meaningful.length ? Math.min(...meaningful.map((item) => item.maxAbs)) : 0;
  const scaleGap = minAbs > 0 ? maxAbs / minAbs : 1;
  const hasPercent = stats.some((item) => item.format === 'percent');
  const hasNonPercent = stats.some((item) => item.format !== 'percent');

  let useDualAxis = false;
  const rightKeys = new Set();
  if (stats.length >= 2 && ((hasPercent && hasNonPercent) || scaleGap >= 8)) {
    useDualAxis = true;
    if (hasPercent && hasNonPercent) {
      for (const item of stats) {
        if (item.format === 'percent') rightKeys.add(item.key);
      }
    } else {
      const sorted = [...stats].sort((a, b) => a.maxAbs - b.maxAbs);
      if (sorted.length > 0) {
        rightKeys.add(sorted[0].key);
      }
    }
  }

  const axisByKey = {};
  for (const series of spec.series) {
    axisByKey[series.key] = useDualAxis && rightKeys.has(series.key) ? 'right' : 'left';
  }

  const leftSeries = spec.series.find((series) => axisByKey[series.key] === 'left') || spec.series[0];
  const rightSeries = spec.series.find((series) => axisByKey[series.key] === 'right') || null;
  return {
    useDualAxis,
    axisByKey,
    leftFormat: leftSeries?.format || 'number',
    rightFormat: rightSeries?.format || leftSeries?.format || 'number',
    leftLabel: leftSeries?.label || '지표',
    rightLabel: rightSeries?.label || '',
  };
}

function renderSeries(spec, axisPlan, showAssistLabels) {
  if (spec.chartType === 'bar') {
    return spec.series.map((series) => (
      <Bar
        key={series.key}
        dataKey={series.key}
        yAxisId={axisPlan.axisByKey[series.key]}
        name={series.label}
        fill={series.color}
        radius={[8, 8, 0, 0]}
        maxBarSize={34}
      >
        {showAssistLabels ? (
          <LabelList
            dataKey={series.key}
            position="top"
            fill="rgba(226, 232, 240, 0.92)"
            fontSize={11}
            formatter={(value) => formatAssistLabel(value, series.format)}
          />
        ) : null}
      </Bar>
    ));
  }

  if (spec.chartType === 'area') {
    return spec.series.map((series, idx) => (
      <Area
        key={series.key}
        type="monotone"
        dataKey={series.key}
        yAxisId={axisPlan.axisByKey[series.key]}
        name={series.label}
        stroke={series.color}
        fill={`url(#chart-grad-${idx})`}
        fillOpacity={1}
        strokeWidth={2.4}
      >
        {showAssistLabels ? (
          <LabelList
            dataKey={series.key}
            position="top"
            fill="rgba(226, 232, 240, 0.92)"
            fontSize={11}
            formatter={(value) => formatAssistLabel(value, series.format)}
          />
        ) : null}
      </Area>
    ));
  }

  return spec.series.map((series) => (
    <Line
      key={series.key}
      type="monotone"
      dataKey={series.key}
      yAxisId={axisPlan.axisByKey[series.key]}
      name={series.label}
      stroke={series.color}
      strokeWidth={2.4}
      dot={{ r: 2.4 }}
      activeDot={{ r: 5 }}
    >
      {showAssistLabels ? (
        <LabelList
          dataKey={series.key}
          position="top"
          fill="rgba(226, 232, 240, 0.92)"
          fontSize={11}
          formatter={(value) => formatAssistLabel(value, series.format)}
        />
      ) : null}
    </Line>
  ));
}

export default function MessageChart({ raw }) {
  const spec = useMemo(() => parseChartSpec(raw), [raw]);

  if (!spec) {
    return <div className="chart-fallback">차트 데이터를 해석하지 못했습니다.</div>;
  }

  const seriesByKey = Object.fromEntries(spec.series.map((series) => [series.key, series]));
  const axisPlan = buildAxisPlan(spec);
  const showAssistLabels = axisPlan.useDualAxis || spec.data.length <= 10;

  const commonProps = {
    data: spec.data,
    margin: { top: 10, right: 14, left: 10, bottom: 8 },
  };

  return (
    <section className="chart-block">
      <div className="chart-head">
        <h4>{spec.title}</h4>
        {spec.subtitle ? <p>{spec.subtitle}</p> : null}
      </div>
      <div className="chart-canvas">
        <ResponsiveContainer width="100%" height={280}>
          {spec.chartType === 'bar' ? (
            <BarChart {...commonProps}>
              <CartesianGrid stroke="rgba(148, 163, 184, 0.24)" strokeDasharray="3 3" />
              <XAxis dataKey={spec.xKey} tick={{ fill: '#cbd5e1', fontSize: 12 }} />
              <YAxis
                yAxisId="left"
                tick={{ fill: '#cbd5e1', fontSize: 12 }}
                tickFormatter={(value) => formatTickByFormat(value, axisPlan.leftFormat)}
              />
              {axisPlan.useDualAxis ? (
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tick={{ fill: '#f1f5f9', fontSize: 12 }}
                  tickFormatter={(value) => formatTickByFormat(value, axisPlan.rightFormat)}
                />
              ) : null}
              <Tooltip
                content={(props) => <CustomTooltip {...props} seriesByKey={seriesByKey} />}
                cursor={{ fill: 'rgba(56, 189, 248, 0.08)' }}
              />
              <Legend wrapperStyle={{ color: '#e2e8f0', fontSize: 12 }} />
              {renderSeries(spec, axisPlan, showAssistLabels)}
            </BarChart>
          ) : spec.chartType === 'area' ? (
            <AreaChart {...commonProps}>
              <defs>
                {spec.series.map((series, idx) => (
                  <linearGradient key={series.key} id={`chart-grad-${idx}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={series.color} stopOpacity={0.45} />
                    <stop offset="95%" stopColor={series.color} stopOpacity={0.04} />
                  </linearGradient>
                ))}
              </defs>
              <CartesianGrid stroke="rgba(148, 163, 184, 0.24)" strokeDasharray="3 3" />
              <XAxis dataKey={spec.xKey} tick={{ fill: '#cbd5e1', fontSize: 12 }} />
              <YAxis
                yAxisId="left"
                tick={{ fill: '#cbd5e1', fontSize: 12 }}
                tickFormatter={(value) => formatTickByFormat(value, axisPlan.leftFormat)}
              />
              {axisPlan.useDualAxis ? (
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tick={{ fill: '#f1f5f9', fontSize: 12 }}
                  tickFormatter={(value) => formatTickByFormat(value, axisPlan.rightFormat)}
                />
              ) : null}
              <Tooltip
                content={(props) => <CustomTooltip {...props} seriesByKey={seriesByKey} />}
                cursor={{ stroke: 'rgba(148, 163, 184, 0.5)' }}
              />
              <Legend wrapperStyle={{ color: '#e2e8f0', fontSize: 12 }} />
              {renderSeries(spec, axisPlan, showAssistLabels)}
            </AreaChart>
          ) : (
            <LineChart {...commonProps}>
              <CartesianGrid stroke="rgba(148, 163, 184, 0.24)" strokeDasharray="3 3" />
              <XAxis dataKey={spec.xKey} tick={{ fill: '#cbd5e1', fontSize: 12 }} />
              <YAxis
                yAxisId="left"
                tick={{ fill: '#cbd5e1', fontSize: 12 }}
                tickFormatter={(value) => formatTickByFormat(value, axisPlan.leftFormat)}
              />
              {axisPlan.useDualAxis ? (
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tick={{ fill: '#f1f5f9', fontSize: 12 }}
                  tickFormatter={(value) => formatTickByFormat(value, axisPlan.rightFormat)}
                />
              ) : null}
              <Tooltip
                content={(props) => <CustomTooltip {...props} seriesByKey={seriesByKey} />}
                cursor={{ stroke: 'rgba(148, 163, 184, 0.5)' }}
              />
              <Legend wrapperStyle={{ color: '#e2e8f0', fontSize: 12 }} />
              {renderSeries(spec, axisPlan, showAssistLabels)}
            </LineChart>
          )}
        </ResponsiveContainer>
      </div>
      <div className="chart-unit">
        {axisPlan.useDualAxis
          ? `좌축: ${axisPlan.leftLabel} · 우축: ${axisPlan.rightLabel}`
          : `기준 지표: ${axisPlan.leftLabel}`}
      </div>
    </section>
  );
}
