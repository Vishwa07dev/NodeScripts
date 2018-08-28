function sample(value){
  console.log("hello "+value);

}
//A function can take any number of arguments irrespective of it's defnition.
sample();

function sayHello(label){
   console.log(label +" "+ this.name);
}

var person1 = {
     name : "Vishwa",
     sayHi : sayHello
}

var person2 = {
     name : "Mohan",
     sayHi : sayHello
}


var name = "roshan" ;
console.log("this"+ this);
console.log(this.name + " -- this.name ");
sayHello.call(this,"global");
sayHello.call(person1, "person1");
sayHello.call(person2 , "person2");

