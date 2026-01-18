import {
  Bytes
} from "@firebase/firestore";

/**
 * Helper to convert Uint8Array to Base64 string (for manual debugging or legacy reasons)
 */
export const Uint8ArrayToBase64 = async (buffer: Uint8Array) => {
  const base64url = await new Promise((r) => {
    const reader = new FileReader();
    reader.onload = () => r(reader.result);
    reader.readAsDataURL(new Blob([buffer]));
  });
  // remove the `data:...;base64,` part from the start
  const bas64: string = base64url as string;
  return bas64.slice(bas64.indexOf(",") + 1);
};

/**
 * Helper to convert Base64 string to Uint8Array
 */
export const base64ToUint8Array = async (base64: string) => {
  var dataUrl = "data:application/octet-binary;base64," + base64;

  const uint8 = await fetch(dataUrl)
    .then((res) => res.arrayBuffer())
    .then((buffer) => new Uint8Array(buffer));

  return uint8;
};
