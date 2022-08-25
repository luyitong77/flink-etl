package com.bdilab.flinketl.utils.kafka;

import java.io.EOFException;

/**
 * @description: java.io.DataInputStream/DataOutputStream
 * @author: ljw
 * @time: 2021/9/23 20:21
 */
public class SerializationUtil {

    /**
     * @param v
     * @return
     */
    public static byte[] intToBytes(int v) {
//        byte[] bytes = new byte[4];
//        bytes[0] = (byte)((v >>> 24) & 0xFF);
//        bytes[0] = (byte)((v >>> 16) & 0xFF);
//        bytes[0] = (byte)((v >>>  8) & 0xFF);
//        bytes[0] = (byte)((v >>>  0) & 0xFF);
//        return bytes;

        byte[] bytes = new byte[4];
        bytes[0] = (byte) (v >> 24 & 0xFF);
        bytes[1] = (byte) (v >> 16 & 0xFF);
        bytes[2] = (byte) (v >> 8 & 0xFF);
        bytes[3] = (byte) (v & 0xFF);
        return bytes;
    }

    /**
     * @param bytes
     * @return
     */
    public static int bytesToInt(byte[] bytes) {
//        int b0 = bytes[0] & 0xFF;
//        int b1 = bytes[1] & 0xFF;
//        int b2 = bytes[2] & 0xFF;
//        int b3 = bytes[3] & 0xFF;
//        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;

        int b0 = bytes[0] & 0xFF;
        int b1 = bytes[1] & 0xFF;
        int b2 = bytes[2] & 0xFF;
        int b3 = bytes[3] & 0xFF;
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }

    /**
     * @param v
     * @return
     */
    public static byte[] longToBytes(long v) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) (v >>> 56);
        bytes[1] = (byte) (v >>> 48);
        bytes[2] = (byte) (v >>> 40);
        bytes[3] = (byte) (v >>> 32);
        bytes[4] = (byte) (v >>> 24);
        bytes[5] = (byte) (v >>> 16);
        bytes[6] = (byte) (v >>> 8);
        bytes[7] = (byte) (v >>> 0);
        return bytes;
    }

    /**
     * @param bytes
     * @return
     */
    public static long bytesToLong(byte[] bytes) {
        return (((long) bytes[0] << 56) +
                ((long) (bytes[1] & 255) << 48) +
                ((long) (bytes[2] & 255) << 40) +
                ((long) (bytes[3] & 255) << 32) +
                ((long) (bytes[4] & 255) << 24) +
                ((bytes[5] & 255) << 16) +
                ((bytes[6] & 255) << 8) +
                ((bytes[7] & 255) << 0));
    }

    /**
     * @param v
     * @return
     */
    public static byte[] floatToBytes(float v) {
        return intToBytes(Float.floatToIntBits(v));
    }

    /**
     * @param bytes
     * @return
     */
    public static float bytesToFloat(byte[] bytes) {
        return Float.intBitsToFloat(bytesToInt(bytes));
    }

    /**
     * @param v
     * @return
     */
    public static byte[] doubleToBytes(double v) {
        return longToBytes(Double.doubleToLongBits(v));
    }

    /**
     * @param bytes
     * @return
     */
    public static double bytesToDouble(byte[] bytes) {
        return Double.longBitsToDouble(bytesToLong(bytes));
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static String bytesToString(byte[] bytes) {
        return new String(bytes);
    }
}