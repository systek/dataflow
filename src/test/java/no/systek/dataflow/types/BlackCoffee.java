package no.systek.dataflow.types;

public class BlackCoffee {

    public final GrindedCoffee grindedCoffee;
    public final Water water;

    public BlackCoffee(GrindedCoffee grindedCoffee, Water water) {
        this.grindedCoffee = grindedCoffee;
        this.water = water;
    }
}
