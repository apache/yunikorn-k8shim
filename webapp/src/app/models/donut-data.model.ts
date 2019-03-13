export class DonutDataItem {
    public name: string;
    public y: number;
    public color: string;

    constructor(name: string, value: number, color: string) {
        this.name = name;
        this.y = value;
        this.color = color;
    }
}
