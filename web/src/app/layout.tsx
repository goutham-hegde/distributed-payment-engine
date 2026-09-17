import type { Metadata } from "next";
import { Spectral, Libre_Franklin, IBM_Plex_Mono } from "next/font/google";
import "./globals.css";

const serif = Spectral({
  subsets: ["latin"],
  weight: ["500", "600"],
  variable: "--font-serif",
  display: "swap",
});
const sans = Libre_Franklin({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  variable: "--font-sans",
  display: "swap",
});
const mono = IBM_Plex_Mono({
  subsets: ["latin"],
  weight: ["400", "500"],
  variable: "--font-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: "Distributed Payment Engine",
  description:
    "A fault-tolerant money-transfer system across three services, running live: real Postgres, a real double-entry ledger, a real saga, and fault switches you can throw yourself.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${serif.variable} ${sans.variable} ${mono.variable}`}>
      <body>{children}</body>
    </html>
  );
}
