'use client';

import { usePathname } from 'next/navigation';
import { useEffect } from 'react';

const ENTER_CLASS = 'page-enter';
const LEAVE_CLASS = 'page-leave';

export default function PageTransitionController() {
  const pathname = usePathname();

  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove(LEAVE_CLASS);
    root.classList.add(ENTER_CLASS);

    const timer = window.setTimeout(() => {
      root.classList.remove(ENTER_CLASS);
    }, 340);

    return () => window.clearTimeout(timer);
  }, [pathname]);

  return null;
}
