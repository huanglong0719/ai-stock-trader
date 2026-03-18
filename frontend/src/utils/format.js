export const formatVolume = (vol) => {
  if (!vol) return '--';
  if (vol > 100000000) return (vol / 100000000).toFixed(2) + '亿手';
  if (vol > 10000) return (vol / 10000).toFixed(2) + '万手';
  return Math.round(vol) + '手';
};

export const formatAmount = (amount) => {
  if (!amount) return '--';
  if (amount > 100000000) return (amount / 100000000).toFixed(2) + '亿';
  if (amount > 10000) return (amount / 10000).toFixed(2) + '万';
  return Math.round(amount) + '元';
};

export const formatHandsFromShares = (shares) => {
  if (shares === null || shares === undefined) return '--';
  const v = Number(shares);
  if (!Number.isFinite(v)) return '--';
  const hands = v / 100;
  const text = Number.isInteger(hands) ? String(hands) : hands.toFixed(2).replace(/\.?0+$/, '');
  return `${text}手`;
};
