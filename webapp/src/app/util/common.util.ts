export class CommonUtils {
    static createUniqId(prefix?: string) {
        const uniqid = Math.random()
            .toString(36)
            .substr(2);
        if (prefix) {
            return prefix + uniqid;
        }
        return uniqid;
    }

    static formatMemory(value: number) {
        let toUnit = 'MB';
        let toValue = value;
        // if (toValue / 1024 >= 0.9) {
        //     toValue = toValue / 1024;
        //     toUnit = 'KB';
        // }
        // if (toValue / 1024 >= 0.9) {
        //     toValue = toValue / 1024;
        //     toUnit = 'MB';
        // }
        if (toValue / 1024 >= 0.9) {
            toValue = toValue / 1024;
            toUnit = 'GB';
        }
        if (toValue / 1024 >= 0.9) {
            toValue = toValue / 1024;
            toUnit = 'TB';
        }
        if (toValue / 1024 >= 0.9) {
            toValue = toValue / 1024;
            toUnit = 'PB';
        }
        return toValue.toFixed(1) + ' ' + toUnit;
    }
}
