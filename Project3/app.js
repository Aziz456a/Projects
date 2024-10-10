
//Linking each page element with a variable by their ids
const computerchoicedis = document.getElementById('computer-choice');
const userchoicedisp = document.getElementById('user-choice');
const resultdisp = document.getElementById('result');
const possiblechoices = document.querySelectorAll('button');
//variables to hold user and computer choices
let userChoice;
let computerchoice;
let res;

//link the clicking event to actions for each button
possiblechoices.forEach(pc => pc.addEventListener('click',(e) => {
    userChoice = e.target.id;
    userchoicedisp.innerHTML = userChoice;
    generateComputerChoice();
    getResult();

}))

//associating each label to a randomly generated number to have the computer choice
function generateComputerChoice()
{

   const randNumber = Math.floor(Math.random() * possiblechoices.length)+1
   //console.log(randNumber);
   switch(randNumber)
   {
    case 1:
        computerchoice = "rock";
        break;
        
    case 2:
        computerchoice = "scissors";
        break;
       
    case 3:
        computerchoice ="paper";
        break;
        
  }

  computerchoicedis.innerHTML=computerchoice;
  }


function getResult()
{
    if(computerchoice == userChoice)
    {
        res = "Draw!";
    }
    if((computerchoice == "rock") &&(userChoice == "paper"))
    {
        res= "User wins!";
    }
    if((computerchoice == "paper") &&(userChoice == "rock"))
    {
        res= "Computer wins!";
    }
    if((computerchoice == "scissors") &&(userChoice == "paper"))
    {
        res= "Computer wins!";
    }
    if((computerchoice == "paper") &&(userChoice == "scissors"))
    {
        res= "User wins!";
    }
    if((computerchoice == "rock") &&(userChoice == "scissors"))
    {
        res= "Computer wins!";
    }
    if((computerchoice == "scissors") &&(userChoice == "rock"))
    {
        res= "User wins!";
    }
  

    resultdisp.innerHTML = res;
}
