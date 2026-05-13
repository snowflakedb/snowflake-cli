import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Snowflake Apps Next.js IT",
  description: "Minimal Next.js app for snow app deploy integration tests",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
