import './globals.css';

export const metadata = {
  title: 'Review Analyst Chat',
  description: 'Vercel-ready review analytics chat frontend',
};

export default function RootLayout({ children }) {
  return (
    <html lang="ko">
      <body>{children}</body>
    </html>
  );
}
