'use client';

import { useRouter } from 'next/navigation';

const LEAVE_ANIMATION_MS = 220;

function isInternalHref(href) {
  return typeof href === 'string' && href.startsWith('/') && !href.startsWith('//');
}

function isModifiedClick(event) {
  return event.metaKey || event.ctrlKey || event.shiftKey || event.altKey;
}

export default function AnimatedNavLink({ href, className, children, onClick, ...props }) {
  const router = useRouter();

  const handleClick = (event) => {
    onClick?.(event);
    if (event.defaultPrevented) return;

    if (!isInternalHref(href) || event.button !== 0 || isModifiedClick(event)) {
      return;
    }

    event.preventDefault();
    const root = document.documentElement;
    root.classList.remove('page-enter');
    root.classList.add('page-leave');

    window.setTimeout(() => {
      router.push(href);
    }, LEAVE_ANIMATION_MS);
  };

  return (
    <a href={href} className={className} onClick={handleClick} {...props}>
      {children}
    </a>
  );
}
