'use client';

import { useEffect, useMemo, useState } from 'react';

const REVIEW_PHASES = [
  '질문 의도를 파악하는 중',
  '리뷰 패턴을 정리하는 중',
  '핵심 인사이트를 구성하는 중',
  '답변 문장을 다듬는 중',
];

const SALES_PHASES = [
  '질문 의도를 파악하는 중',
  '매출 지표를 집계하는 중',
  '비교 포인트를 계산하는 중',
  '답변 문장을 다듬는 중',
];

export default function LiveThinking({ mode = 'review' }) {
  const phases = mode === 'sales' ? SALES_PHASES : REVIEW_PHASES;
  const [phaseIndex, setPhaseIndex] = useState(0);
  const [dotCount, setDotCount] = useState(1);
  const [elapsedSec, setElapsedSec] = useState(0);

  useEffect(() => {
    const dotTimer = window.setInterval(() => {
      setDotCount((prev) => (prev >= 3 ? 1 : prev + 1));
    }, 260);

    const phaseTimer = window.setInterval(() => {
      setPhaseIndex((prev) => (prev + 1) % phases.length);
    }, 1400);

    const elapsedTimer = window.setInterval(() => {
      setElapsedSec((prev) => prev + 1);
    }, 1000);

    return () => {
      window.clearInterval(dotTimer);
      window.clearInterval(phaseTimer);
      window.clearInterval(elapsedTimer);
    };
  }, [phases.length]);

  const dots = useMemo(() => '.'.repeat(dotCount), [dotCount]);

  return (
    <div className="live-thinking" aria-live="polite">
      <p className="live-thinking-line">
        <span className="live-thinking-pulse" aria-hidden />
        <span>{phases[phaseIndex]}</span>
        <span className="live-thinking-dots">{dots}</span>
      </p>
      <p className="live-thinking-meta">실시간 생성 중 · {elapsedSec}s 경과</p>
      <div className="live-thinking-bars" aria-hidden>
        <span />
        <span />
        <span />
      </div>
    </div>
  );
}
